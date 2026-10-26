#!/usr/bin/env bash
# =============================================================================
# HTTP contract checks for pricing, market-data, and blotter.
#
# Pytest stays on worker and cache logic (it calls the Bottle app directly).
# This script is the gate when those services move from Bottle to FastAPI:
# same paths, status codes, and JSON bodies, no framework imports.
#
# Prerequisites:
#   - The three services running (docker compose up). /books/summary also
#     needs books-service unless EXPECT_BOOKS_DOWN=1.
#   - jq installed.
# Usage:
#   bash tests/curl_contract.sh
#   PRICING_URL=http://localhost:9002 bash tests/curl_contract.sh
# =============================================================================

set -euo pipefail

PRICING_URL="${PRICING_URL:-http://localhost:8002}"
MARKET_DATA_URL="${MARKET_DATA_URL:-http://localhost:8001}"
BLOTTER_URL="${BLOTTER_URL:-http://localhost:8006}"
NIL_ID="00000000-0000-0000-0000-000000000000"
EXPECT_BOOKS_DOWN="${EXPECT_BOOKS_DOWN:-0}"

PASS=0
FAIL=0
HTTP_STATUS=""

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
HEADER_FILE="$TMP_DIR/headers"
BODY_FILE="$TMP_DIR/body"

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required. Install with: brew install jq"
  exit 1
fi

section() {
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "══════════════════════════════════════════════════════════════"
}

pass() {
  echo "  [PASS] $1"
  PASS=$((PASS + 1))
}

fail() {
  echo "  [FAIL] $1"
  if [[ -s "$BODY_FILE" ]]; then
    echo "         body: $(head -c 300 "$BODY_FILE")"
  fi
  FAIL=$((FAIL + 1))
}

# fetch <url> [max-time-seconds]
# SSE calls pass a max time. curl exits 28 when that timer fires after a
# live stream; the HTTP status is still captured.
fetch() {
  local url="$1"
  local max_time="${2:-}"
  local -a args=(-s -D "$HEADER_FILE" -o "$BODY_FILE" -w "%{http_code}")
  : >"$HEADER_FILE"
  : >"$BODY_FILE"
  if [[ -n "$max_time" ]]; then
    args+=(-N --max-time "$max_time")
  fi
  HTTP_STATUS=$(curl "${args[@]}" "$url" || true)
}

expect_status() {
  local label="$1"
  local expected="$2"
  if [[ "$HTTP_STATUS" == "$expected" ]]; then
    pass "$label → HTTP $HTTP_STATUS"
  else
    fail "$label → expected HTTP $expected, got HTTP ${HTTP_STATUS:-none}"
  fi
}

expect_content_type() {
  local label="$1"
  local prefix="$2"
  local raw lower_value lower_prefix
  raw=$(grep -i '^content-type:' "$HEADER_FILE" | head -1 | tr -d '\r' || true)
  lower_value=$(printf '%s' "$raw" | cut -d: -f2- | sed 's/^[[:space:]]*//' | tr '[:upper:]' '[:lower:]')
  lower_prefix=$(printf '%s' "$prefix" | tr '[:upper:]' '[:lower:]')
  if [[ -n "$lower_value" && "$lower_value" == "$lower_prefix"* ]]; then
    pass "$label → Content-Type $lower_value"
  else
    fail "$label → expected Content-Type starting with $prefix, got ${raw:-<missing>}"
  fi
}

expect_jq() {
  local label="$1"
  shift
  if jq -e "$@" "$BODY_FILE" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label"
  fi
}

expect_error() {
  local label="$1"
  local expected="$2"
  if jq -e --arg expected "$expected" '.error == $expected' "$BODY_FILE" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label"
  fi
}

expect_error_prefix() {
  local label="$1"
  local prefix="$2"
  if jq -e --arg prefix "$prefix" '.error | type == "string" and startswith($prefix)' "$BODY_FILE" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label"
  fi
}

body_has_text() {
  [[ -n "$(tr -d '[:space:]' < "$BODY_FILE")" ]]
}

# ── pricing-service ───────────────────────────────────────────────────────────
section "pricing-service"

fetch "$PRICING_URL/health"
expect_status "GET /health" "200"
expect_content_type "GET /health" "application/json"
expect_jq "GET /health service" '.service == "pricing-service"'
expect_jq "GET /health status" '.status == "UP"'

fetch "$PRICING_URL/valuations"
expect_status "GET /valuations" "200"
expect_content_type "GET /valuations" "application/json"
expect_jq "GET /valuations is an object" 'type == "object"'
VAL_ID=$(jq -r 'if type == "object" and (keys | length) > 0 then keys[0] else empty end' "$BODY_FILE" 2>/dev/null || true)

fetch "$PRICING_URL/valuations/$NIL_ID"
expect_status "GET /valuations/<unknown>" "404"
expect_content_type "GET /valuations/<unknown>" "application/json"
expect_error "GET /valuations/<unknown> error" "Valuation not found for trade_id: $NIL_ID"

if [[ -n "$VAL_ID" ]]; then
  fetch "$PRICING_URL/valuations/$VAL_ID"
  expect_status "GET /valuations/<id>" "200"
  expect_content_type "GET /valuations/<id>" "application/json"
  expect_jq "GET /valuations/<id> trade_id" --arg id "$VAL_ID" '.trade_id == $id'
else
  echo "  [SKIP] GET /valuations/<id> — store is empty"
fi

fetch "$PRICING_URL/book-metrics"
expect_status "GET /book-metrics" "200"
expect_content_type "GET /book-metrics" "application/json"
expect_jq "GET /book-metrics benchmark" '.benchmark == "MARKET_INDEX"'
expect_jq "GET /book-metrics books" '.books | type == "object"'

fetch "$PRICING_URL/book-metrics/$NIL_ID"
expect_status "GET /book-metrics/<unknown>" "404"
expect_content_type "GET /book-metrics/<unknown>" "application/json"
expect_error_prefix "GET /book-metrics/<unknown> error" "No metrics available for book_id:"

fetch "$PRICING_URL/valuation-stream" 3
expect_status "GET /valuation-stream" "200"
expect_content_type "GET /valuation-stream" "text/event-stream"
if [[ "$HTTP_STATUS" != "200" ]]; then
  :
elif body_has_text; then
  if grep -q "event: valuation_update" "$BODY_FILE" && grep -q "data:" "$BODY_FILE"; then
    pass "GET /valuation-stream event frame"
  else
    fail "GET /valuation-stream event frame"
  fi
else
  pass "GET /valuation-stream empty body (no valuations yet)"
fi

# ── market-data-service ───────────────────────────────────────────────────────
section "market-data-service"

fetch "$MARKET_DATA_URL/health"
expect_status "GET /health" "200"
expect_content_type "GET /health" "application/json"
expect_jq "GET /health service" '.service == "market-data-service"'
expect_jq "GET /health status" '.status == "UP"'
expect_jq "GET /health active_subscribers" '.active_subscribers | type == "number"'

fetch "$MARKET_DATA_URL/symbols"
expect_status "GET /symbols" "200"
expect_content_type "GET /symbols" "application/json"
expect_jq "GET /symbols is an object" 'type == "object"'

fetch "$MARKET_DATA_URL/snapshot"
expect_status "GET /snapshot" "200"
expect_content_type "GET /snapshot" "application/json"
expect_jq "GET /snapshot is an object" 'type == "object"'

fetch "$MARKET_DATA_URL/stream" 5
expect_status "GET /stream" "200"
expect_content_type "GET /stream" "text/event-stream"
if grep -q "data:" "$BODY_FILE"; then
  pass "GET /stream received a data line"
else
  fail "GET /stream received a data line"
fi

# ── blotter-service ───────────────────────────────────────────────────────────
section "blotter-service"

fetch "$BLOTTER_URL/health"
expect_status "GET /health" "200"
expect_content_type "GET /health" "application/json"
expect_jq "GET /health service" '.service == "blotter-service"'
expect_jq "GET /health status" '.status == "UP"'
expect_jq "GET /health cache_trade_count" '.cache_trade_count | type == "number"'
expect_jq "GET /health cache_entry_count" '.cache_entry_count | type == "number"'

fetch "$BLOTTER_URL/books/summary"
if [[ "$EXPECT_BOOKS_DOWN" == "1" ]]; then
  expect_status "GET /books/summary (books-service down)" "500"
  expect_content_type "GET /books/summary" "application/json"
  expect_error "GET /books/summary error" "Failed to fetch book summary"
else
  expect_status "GET /books/summary" "200"
  expect_content_type "GET /books/summary" "application/json"
  expect_jq "GET /books/summary books" '.books | type == "array"'
fi

fetch "$BLOTTER_URL/trades"
expect_status "GET /trades" "200"
expect_content_type "GET /trades" "application/json"
expect_jq "GET /trades is a list" '.trades | type == "array"'
TRADE_ID=$(jq -r 'if (.trades | type) == "array" and (.trades | length) > 0 then .trades[0].trade_id // empty else empty end' "$BODY_FILE" 2>/dev/null || true)

fetch "$BLOTTER_URL/trades?book_id=b&asset_class=EQUITY&status=ACTIVE&symbol=ACME&first_only=yes&page=1&limit=5"
expect_status "GET /trades?filters" "200"
expect_content_type "GET /trades?filters" "application/json"
expect_jq "GET /trades?filters is a list" '.trades | type == "array"'

fetch "$BLOTTER_URL/trades/$NIL_ID"
expect_status "GET /trades/<unknown>" "404"
expect_content_type "GET /trades/<unknown>" "application/json"
expect_error "GET /trades/<unknown> error" "Trade not found"

if [[ -n "$TRADE_ID" ]]; then
  fetch "$BLOTTER_URL/trades/$TRADE_ID"
  expect_status "GET /trades/<id>" "200"
  expect_content_type "GET /trades/<id>" "application/json"
  expect_jq "GET /trades/<id> has trade" 'has("trade")'

  fetch "$BLOTTER_URL/trades/$TRADE_ID/valuations"
  expect_status "GET /trades/<id>/valuations" "200"
  expect_content_type "GET /trades/<id>/valuations" "application/json"
  expect_jq "GET /trades/<id>/valuations is a list" '.valuations | type == "array"'
else
  echo "  [SKIP] GET /trades/<id> — no trades on file"
fi

fetch "$BLOTTER_URL/trades/$NIL_ID/audit-logs"
expect_status "GET /trades/<id>/audit-logs" "200"
if [[ "$HTTP_STATUS" != "200" ]]; then
  :
elif body_has_text; then
  fail "GET /trades/<id>/audit-logs empty body"
else
  pass "GET /trades/<id>/audit-logs empty body"
fi

section "SUMMARY"
TOTAL=$((PASS + FAIL))
echo "  Total checks : $TOTAL"
echo "  Passed       : $PASS"
echo "  Failed       : $FAIL"
echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "  ALL CHECKS PASSED"
  exit 0
fi
echo "  $FAIL CHECK(S) FAILED — review output above"
exit 1
