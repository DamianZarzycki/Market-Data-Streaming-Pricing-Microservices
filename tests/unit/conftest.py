import os

if not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"

# Same Compose DNS defaults as backend/docker-compose.yml. Services call
# trading_shared.env.env and fail when a name is missing.
os.environ.setdefault("MARKET_DATA_SERVICE_URL", "http://market-data-service:8001")
os.environ.setdefault("MARKET_DATA_INTEGRATION_URL", "http://market-data-service-integration:8005")
os.environ.setdefault("PRICING_SERVICE_URL", "http://pricing-service:8002")
os.environ.setdefault("BOOKS_SERVICE_URL", "http://books-service:8004")
os.environ.setdefault("TRADE_ACTION_SERVICE_URL", "http://trade-action-service:8080")
os.environ.setdefault("TRADE_GENERATION_SERVICE_URL", "http://trade-generation-service:8007")
os.environ.setdefault("BLOTTER_SERVICE_URL", "http://blotter-service:8006")
