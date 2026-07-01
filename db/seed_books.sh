#!/usr/bin/env bash
# Seed 3 Books via the books-service REST API.
# Usage: bash db/seed_books.sh [BASE_URL]
# Default BASE_URL: http://localhost:8004

BASE_URL="${1:-http://localhost:8004}"

echo "Seeding books against ${BASE_URL} ..."

curl -s -X POST "${BASE_URL}/books" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Global Equities Book",
    "description": "Holds equity positions across global markets.",
    "expected_asset_class": "EQUITY"
  }' | python3 -m json.tool

curl -s -X POST "${BASE_URL}/books" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Fixed Income Book",
    "description": "Holds government and corporate bond positions.",
    "expected_asset_class": "BOND"
  }' | python3 -m json.tool

curl -s -X POST "${BASE_URL}/books" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "FX Book",
    "description": "Holds foreign-exchange spot and forward positions.",
    "expected_asset_class": "FX"
  }' | python3 -m json.tool

echo "Done."
