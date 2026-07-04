#!/usr/bin/env bash
# =============================================================================
# CURL endpoint tests for all market-data-microservices services
# Prerequisites:
#   - All services running:  docker compose up
#   - jq installed:          brew install jq
# Usage:
#   bash tests/curl_tests.sh
# =============================================================================

set -euo pipefail

# ── Base URLs ─────────────────────────────────────────────────────────────────
MARKET_DATA="http://localhost:8001"
PRICING="http://localhost:8002"
MONITORING="http://localhost:8003"
BOOKS="http://localhost:8004"
BLOTTER="http://localhost:8006"
TRADE_GEN="http://localhost:8007"
TRADE_ACTION="http://localhost:8080"

# ── Counters ──────────────────────────────────────────────────────────────────
PASS=0
FAIL=0

# ── Helpers ───────────────────────────────────────────────────────────────────
section() {
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "══════════════════════════════════════════════════════════════"
}

# check_status <label> <expected_http_code> <actual_http_code>
check_status() {
  local label="$1"
  local expected="$2"
  local actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    echo "  [PASS] $label → HTTP $actual"
    PASS=$((PASS + 1))
  else
    echo "  [FAIL] $label → expected HTTP $expected, got HTTP $actual"
    FAIL=$((FAIL + 1))
  fi
}

# curl_get <url>  →  prints body, returns HTTP status in $HTTP_STATUS
curl_get() {
  HTTP_STATUS=$(curl -s -o /tmp/curl_body.json -w "%{http_code}" "$1")
  cat /tmp/curl_body.json | jq . 2>/dev/null || cat /tmp/curl_body.json
  echo ""
}

# curl_post <url> <json_body>  →  prints body, returns HTTP status in $HTTP_STATUS
curl_post() {
  HTTP_STATUS=$(curl -s -o /tmp/curl_body.json -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" -d "$2" "$1")
  cat /tmp/curl_body.json | jq . 2>/dev/null || cat /tmp/curl_body.json
  echo ""
}

# curl_put <url> <json_body>  →  prints body, returns HTTP status in $HTTP_STATUS
curl_put() {
  HTTP_STATUS=$(curl -s -o /tmp/curl_body.json -w "%{http_code}" \
    -X PUT -H "Content-Type: application/json" -d "$2" "$1")
  cat /tmp/curl_body.json | jq . 2>/dev/null || cat /tmp/curl_body.json
  echo ""
}

# curl_delete <url>  →  prints body, returns HTTP status in $HTTP_STATUS
curl_delete() {
  HTTP_STATUS=$(curl -s -o /tmp/curl_body.json -w "%{http_code}" -X DELETE "$1")
  cat /tmp/curl_body.json | jq . 2>/dev/null || cat /tmp/curl_body.json
  echo ""
}

# ── Dependency check ──────────────────────────────────────────────────────────
if ! command -v jq &>/dev/null; then
  echo "ERROR: jq is required. Install with: brew install jq"
  exit 1
fi

# =============================================================================
# PHASE 1 — HEALTH CHECKS
# =============================================================================
section "PHASE 1 — Health Checks"

echo "--- market-data-service ---"
curl_get "$MARKET_DATA/health"
check_status "GET /health (market-data-service)" "200" "$HTTP_STATUS"

echo "--- pricing-service ---"
curl_get "$PRICING/health"
check_status "GET /health (pricing-service)" "200" "$HTTP_STATUS"

echo "--- monitoring-service (SKIPPED: commented out in docker-compose) ---"
echo "  [SKIP] GET /health (monitoring-service)"

echo "--- books-service ---"
curl_get "$BOOKS/health"
check_status "GET /health (books-service)" "200" "$HTTP_STATUS"

echo "--- blotter-service ---"
curl_get "$BLOTTER/health"
check_status "GET /health (blotter-service)" "200" "$HTTP_STATUS"

echo "--- trade-generation-service ---"
curl_get "$TRADE_GEN/health"
check_status "GET /health (trade-generation-service)" "200" "$HTTP_STATUS"

echo "--- trade-action-service ---"
curl_get "$TRADE_ACTION/health"
check_status "GET /health (trade-action-service)" "200" "$HTTP_STATUS"

# =============================================================================
# PHASE 2 — BOOKS SERVICE CRUD
# =============================================================================
section "PHASE 2 — Books Service CRUD"

echo "--- POST /books (single) ---"
curl_post "$BOOKS/books" '{
  "name": "Test Equity Book",
  "description": "Created by curl_tests.sh",
  "expected_asset_class": "EQUITY"
}'
check_status "POST /books (single)" "201" "$HTTP_STATUS"

BOOK_ID=$(cat /tmp/curl_body.json | jq -r '.book_ids[0] // empty')
if [[ -z "$BOOK_ID" ]]; then
  echo "  [WARN] Could not capture BOOK_ID — book may already exist. Fetching from GET /books..."
  curl_get "$BOOKS/books"
  BOOK_ID=$(cat /tmp/curl_body.json | jq -r '.books[0].book_id // empty')
fi
echo "  Captured BOOK_ID: $BOOK_ID"

echo "--- POST /books (array / batch) ---"
curl_post "$BOOKS/books" '[
  {
    "name": "Test Bond Book",
    "description": "Batch created by curl_tests.sh",
    "expected_asset_class": "BOND"
  },
  {
    "name": "Test FX Book",
    "description": "Batch created by curl_tests.sh",
    "expected_asset_class": "FX"
  }
]'
check_status "POST /books (array batch)" "201" "$HTTP_STATUS"

echo "--- GET /books ---"
curl_get "$BOOKS/books"
check_status "GET /books" "200" "$HTTP_STATUS"

echo "--- GET /books/<book_id> ---"
curl_get "$BOOKS/books/$BOOK_ID"
check_status "GET /books/$BOOK_ID" "200" "$HTTP_STATUS"

echo "--- PUT /books/<book_id> ---"
curl_put "$BOOKS/books/$BOOK_ID" '{
  "description": "Updated by curl_tests.sh"
}'
check_status "PUT /books/$BOOK_ID" "200" "$HTTP_STATUS"

echo "--- GET /books/<invalid_id> → 404 ---"
curl_get "$BOOKS/books/00000000-0000-0000-0000-000000000000"
check_status "GET /books/invalid → 404" "404" "$HTTP_STATUS"

echo "--- DELETE /books/<book_id> ---"
# Create a throwaway book to delete
curl_post "$BOOKS/books" '{
  "name": "Throwaway Book",
  "description": "Will be deleted",
  "expected_asset_class": "EQUITY"
}'
THROWAWAY_BOOK_ID=$(cat /tmp/curl_body.json | jq -r '.book_ids[0] // empty')
if [[ -n "$THROWAWAY_BOOK_ID" ]]; then
  curl_delete "$BOOKS/books/$THROWAWAY_BOOK_ID"
  check_status "DELETE /books/$THROWAWAY_BOOK_ID" "200" "$HTTP_STATUS"
else
  echo "  [SKIP] DELETE /books — could not create throwaway book"
fi

# =============================================================================
# PHASE 3 — MARKET DATA SERVICE
# =============================================================================
section "PHASE 3 — Market Data Service"

echo "--- GET /symbols ---"
curl_get "$MARKET_DATA/symbols"
check_status "GET /symbols" "200" "$HTTP_STATUS"

# Capture a known equity symbol for later trade-action tests
EQUITY_SYMBOL=$(cat /tmp/curl_body.json | jq -r '.EQUITY[0] // "AAPL"')
echo "  Using equity symbol for trade tests: $EQUITY_SYMBOL"

echo "--- GET /snapshot ---"
curl_get "$MARKET_DATA/snapshot"
check_status "GET /snapshot" "200" "$HTTP_STATUS"

# =============================================================================
# PHASE 4 — TRADE ACTION SERVICE
# =============================================================================
section "PHASE 4 — Trade Action Service"

CLIENT_REQ_ID_OPEN="req-test-open-$(date +%s)"

echo "--- POST /trade-actions (OPEN_TRADE) ---"
curl_post "$TRADE_ACTION/trade-actions" "{
  \"action_type\": \"OPEN_TRADE\",
  \"client_request_id\": \"$CLIENT_REQ_ID_OPEN\",
  \"book_id\": \"$BOOK_ID\",
  \"asset_class\": \"EQUITY\",
  \"symbol\": \"$EQUITY_SYMBOL\",
  \"side\": \"BUY\",
  \"quantity\": 100,
  \"trade_price\": 150.00,
  \"currency\": \"USD\"
}"
check_status "POST /trade-actions (OPEN_TRADE)" "202" "$HTTP_STATUS"

echo "  Waiting 3s for async trade creation..."
sleep 3

echo "--- GET /trades (blotter) to capture TRADE_ID ---"
curl_get "$BLOTTER/trades?symbol=$EQUITY_SYMBOL&status=ACTIVE&limit=1"
check_status "GET /trades?symbol=$EQUITY_SYMBOL&status=ACTIVE" "200" "$HTTP_STATUS"

TRADE_ID=$(cat /tmp/curl_body.json | jq -r '.trades[0].trade_id // empty')
if [[ -z "$TRADE_ID" ]]; then
  echo "  [WARN] Could not capture TRADE_ID — fetching first available trade..."
  curl_get "$BLOTTER/trades?status=ACTIVE&limit=1"
  TRADE_ID=$(cat /tmp/curl_body.json | jq -r '.trades[0].trade_id // empty')
fi
echo "  Captured TRADE_ID: $TRADE_ID"

CLIENT_REQ_ID_CLOSE="req-test-close-$(date +%s)"

echo "--- POST /trade-actions (CLOSE_TRADE) ---"
if [[ -n "$TRADE_ID" ]]; then
  curl_post "$TRADE_ACTION/trade-actions" "{
    \"action_type\": \"CLOSE_TRADE\",
    \"client_request_id\": \"$CLIENT_REQ_ID_CLOSE\",
    \"trade_id\": \"$TRADE_ID\",
    \"close_price\": 155.00,
    \"symbol\": \"$EQUITY_SYMBOL\",
    \"close_reason\": \"MANUAL\"
  }"
  check_status "POST /trade-actions (CLOSE_TRADE)" "202" "$HTTP_STATUS"
else
  echo "  [SKIP] CLOSE_TRADE — no TRADE_ID available"
fi

echo "--- POST /trade-actions/batch (two OPEN_TRADEs) ---"
curl_post "$TRADE_ACTION/trade-actions/batch" "[
  {
    \"action_type\": \"OPEN_TRADE\",
    \"client_request_id\": \"req-batch-1-$(date +%s)\",
    \"book_id\": \"$BOOK_ID\",
    \"asset_class\": \"EQUITY\",
    \"symbol\": \"$EQUITY_SYMBOL\",
    \"side\": \"SELL\",
    \"quantity\": 50,
    \"trade_price\": 148.50,
    \"currency\": \"USD\"
  },
  {
    \"action_type\": \"OPEN_TRADE\",
    \"client_request_id\": \"req-batch-2-$(date +%s)\",
    \"book_id\": \"$BOOK_ID\",
    \"asset_class\": \"EQUITY\",
    \"symbol\": \"$EQUITY_SYMBOL\",
    \"side\": \"BUY\",
    \"quantity\": 200,
    \"trade_price\": 152.75,
    \"currency\": \"USD\"
  }
]"
check_status "POST /trade-actions/batch" "202" "$HTTP_STATUS"

echo "--- POST /trade-actions (invalid — missing client_request_id → 400) ---"
curl_post "$TRADE_ACTION/trade-actions" '{
  "action_type": "OPEN_TRADE",
  "book_id": "00000000-0000-0000-0000-000000000001"
}'
check_status "POST /trade-actions (invalid → 400)" "400" "$HTTP_STATUS"

echo "--- POST /trade-actions (invalid action_type → 400) ---"
curl_post "$TRADE_ACTION/trade-actions" '{
  "action_type": "INVALID_TYPE",
  "client_request_id": "req-bad-action"
}'
check_status "POST /trade-actions (bad action_type → 400)" "400" "$HTTP_STATUS"

echo "--- POST /trade-actions/batch (non-list payload → 400) ---"
curl_post "$TRADE_ACTION/trade-actions/batch" '{"action_type": "OPEN_TRADE"}'
check_status "POST /trade-actions/batch (non-list → 400)" "400" "$HTTP_STATUS"

# =============================================================================
# PHASE 5 — TRADE GENERATION SERVICE
# =============================================================================
section "PHASE 5 — Trade Generation Service"

echo "--- GET /status ---"
curl_get "$TRADE_GEN/status"
check_status "GET /status (trade-generation)" "200" "$HTTP_STATUS"

echo "--- GET /generate-once ---"
curl_get "$TRADE_GEN/generate-once"
check_status "GET /generate-once" "200" "$HTTP_STATUS"

echo "--- GET /generate-batch ---"
curl_get "$TRADE_GEN/generate-batch"
check_status "GET /generate-batch" "200" "$HTTP_STATUS"

echo "--- POST /start ---"
curl_post "$TRADE_GEN/start" '{}'
check_status "POST /start (trade-generation)" "200" "$HTTP_STATUS"

echo "--- GET /status (should show is_running: true) ---"
curl_get "$TRADE_GEN/status"
check_status "GET /status (is_running check)" "200" "$HTTP_STATUS"
IS_RUNNING=$(cat /tmp/curl_body.json | jq -r '.is_running // false')
if [[ "$IS_RUNNING" == "true" ]]; then
  echo "  [PASS] is_running = true"
  PASS=$((PASS + 1))
else
  echo "  [FAIL] expected is_running = true, got: $IS_RUNNING"
  FAIL=$((FAIL + 1))
fi

echo "--- POST /stop ---"
curl_post "$TRADE_GEN/stop" '{}'
check_status "POST /stop (trade-generation)" "200" "$HTTP_STATUS"

echo "--- POST /start again (idempotency: already stopped, should succeed) ---"
curl_post "$TRADE_GEN/start" '{}'
check_status "POST /start (second call)" "200" "$HTTP_STATUS"

echo "--- POST /start again (already running → 400) ---"
curl_post "$TRADE_GEN/start" '{}'
check_status "POST /start (already running → 400)" "400" "$HTTP_STATUS"

echo "--- POST /stop (cleanup) ---"
curl_post "$TRADE_GEN/stop" '{}'
check_status "POST /stop (cleanup)" "200" "$HTTP_STATUS"

# =============================================================================
# PHASE 6 — BLOTTER SERVICE
# =============================================================================
section "PHASE 6 — Blotter Service"

echo "--- GET /books/summary ---"
curl_get "$BLOTTER/books/summary"
check_status "GET /books/summary" "200" "$HTTP_STATUS"

echo "--- GET /trades (no filters) ---"
curl_get "$BLOTTER/trades"
check_status "GET /trades (no filter)" "200" "$HTTP_STATUS"

echo "--- GET /trades?status=ACTIVE ---"
curl_get "$BLOTTER/trades?status=ACTIVE"
check_status "GET /trades?status=ACTIVE" "200" "$HTTP_STATUS"

echo "--- GET /trades?asset_class=EQUITY ---"
curl_get "$BLOTTER/trades?asset_class=EQUITY"
check_status "GET /trades?asset_class=EQUITY" "200" "$HTTP_STATUS"

echo "--- GET /trades?book_id=<BOOK_ID> ---"
curl_get "$BLOTTER/trades?book_id=$BOOK_ID"
check_status "GET /trades?book_id=$BOOK_ID" "200" "$HTTP_STATUS"

echo "--- GET /trades?symbol=<SYMBOL> ---"
curl_get "$BLOTTER/trades?symbol=$EQUITY_SYMBOL"
check_status "GET /trades?symbol=$EQUITY_SYMBOL" "200" "$HTTP_STATUS"

echo "--- GET /trades?page=1&limit=5 ---"
curl_get "$BLOTTER/trades?page=1&limit=5"
check_status "GET /trades?page=1&limit=5" "200" "$HTTP_STATUS"

if [[ -n "$TRADE_ID" ]]; then
  echo "--- GET /trades/<trade_id> ---"
  curl_get "$BLOTTER/trades/$TRADE_ID"
  check_status "GET /trades/$TRADE_ID" "200" "$HTTP_STATUS"

  echo "--- GET /trades/<trade_id>/valuations ---"
  curl_get "$BLOTTER/trades/$TRADE_ID/valuations"
  check_status "GET /trades/$TRADE_ID/valuations" "200" "$HTTP_STATUS"
else
  echo "  [SKIP] /trades/<trade_id> tests — no TRADE_ID captured"
fi

echo "--- GET /trades/<invalid_id> → 404 ---"
curl_get "$BLOTTER/trades/00000000-0000-0000-0000-000000000000"
check_status "GET /trades/invalid → 404" "404" "$HTTP_STATUS"

# =============================================================================
# PHASE 7 — PRICING SERVICE
# =============================================================================
section "PHASE 7 — Pricing Service"

echo "--- GET /valuations ---"
curl_get "$PRICING/valuations"
check_status "GET /valuations" "200" "$HTTP_STATUS"

if [[ -n "$TRADE_ID" ]]; then
  echo "--- GET /valuations/<trade_id> ---"
  curl_get "$PRICING/valuations/$TRADE_ID"
  # 200 if pricing has run, 404 if not yet — both are valid
  if [[ "$HTTP_STATUS" == "200" || "$HTTP_STATUS" == "404" ]]; then
    echo "  [PASS] GET /valuations/$TRADE_ID → HTTP $HTTP_STATUS (200 or 404 acceptable)"
    PASS=$((PASS + 1))
  else
    echo "  [FAIL] GET /valuations/$TRADE_ID → unexpected HTTP $HTTP_STATUS"
    FAIL=$((FAIL + 1))
  fi
else
  echo "  [SKIP] GET /valuations/<trade_id> — no TRADE_ID captured"
fi

# =============================================================================
# PHASE 8 — SSE STREAMS (time-boxed to 5s each)
# =============================================================================
section "PHASE 8 — SSE Streams (5s timeout each)"

echo "--- GET /stream (market-data SSE) ---"
echo "  Connecting for 5 seconds..."
SSE_OUTPUT=$(curl -s --max-time 5 "$MARKET_DATA/stream" 2>&1 || true)
if echo "$SSE_OUTPUT" | grep -q "data:"; then
  echo "  [PASS] GET /stream received at least one SSE event"
  PASS=$((PASS + 1))
else
  echo "  [FAIL] GET /stream — no 'data:' line received within 5s"
  echo "  Raw output: ${SSE_OUTPUT:0:300}"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "--- GET /valuation-stream (pricing SSE) ---"
echo "  Connecting for 5 seconds..."
SSE_VAL_OUTPUT=$(curl -s --max-time 5 "$PRICING/valuation-stream" 2>&1 || true)
if echo "$SSE_VAL_OUTPUT" | grep -q "data:"; then
  echo "  [PASS] GET /valuation-stream received at least one SSE event"
  PASS=$((PASS + 1))
else
  echo "  [FAIL] GET /valuation-stream — no 'data:' line received within 5s"
  echo "  Raw output: ${SSE_VAL_OUTPUT:0:300}"
  FAIL=$((FAIL + 1))
fi

# =============================================================================
# SUMMARY
# =============================================================================
section "SUMMARY"
TOTAL=$((PASS + FAIL))
echo "  Total checks : $TOTAL"
echo "  Passed       : $PASS"
echo "  Failed       : $FAIL"
echo "  Skipped      : monitoring-service (commented out in docker-compose)"
echo ""
if [[ "$FAIL" -eq 0 ]]; then
  echo "  ALL CHECKS PASSED"
  exit 0
else
  echo "  $FAIL CHECK(S) FAILED — review output above"
  exit 1
fi
