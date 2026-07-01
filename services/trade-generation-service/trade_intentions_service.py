import datetime
import random
import uuid
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType, TradeStatus
from shared.trading_shared.models import Trade
import logging
import urllib.request
import json

# TODO threading event moze? (wait)
generator_state = {"is_running": False, "total_generated": 0, "thread": None}

# jak constrainy moze wplynac na transakcje DB -> duze tabele i sprawdzanie id w trakcie insertow
# profiling

def fetch_json(url, method="GET", headers=None, data=None):
    if headers is None:
        headers = {}

    try:
        req = urllib.request.Request(url, method=method, headers=headers, data=data)
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        logging.error(f"Failed to fetch data from {url}: {e}")
        return None

def send_to_trade_action_service(payload):
    if isinstance(payload, list):
        url = "http://trade-action-service:8080/trade-actions/batch"
        message = f"batch of {len(payload)} orders"
    else:
        url = "http://trade-action-service:8080/trade-actions"
        message = "single order"

    response = fetch_json(
        url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )

    if response is not None:
        logging.info(f"Successfully sent {message} to trade-action-service.")
    else:
        logging.error(f"Failed to send {message} to trade-action-service.")


def generate_open_or_close_trade():
    return random.choices(
        [ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value], weights=[0.7, 0.3]
    )[0]

def generate_and_send_to_trade_action_service_batch(count=10):
    batch_payload = []

    for _ in range(count):
        intention = generate_random_intention()
        if intention:
            batch_payload.append(intention)
            generator_state["total_generated"] += 1

    if batch_payload:
        send_to_trade_action_service(batch_payload)
        return batch_payload
        
    logging.info("Couldn't generate any intentions for batch.")
    return None


def generate_and_send_to_trade_action_service():
    intention = generate_random_intention()

    if intention:
        generator_state["total_generated"] += 1
        send_to_trade_action_service(intention)
        logging.info(
            f"[Trade Generation] Generated intention {generator_state['total_generated']}."
        )

        return intention
    else:
        logging.info("No intention generated.")
        return None


def generate_request_id():
    return f"req-{datetime.datetime.now().strftime('%Y%m%d')}-{uuid.uuid4()}"


def generate_random_intention():
    with DBSessionManager() as db:
        try:
            action = generate_open_or_close_trade()
            logging.info(f"Generated action: {action} == {ActionType.CLOSE_TRADE.value}")

            if action == ActionType.CLOSE_TRADE.value:
                logging.info("Checking for active trades to close...")
                active_trades = db.trades.get_trades(None, status=TradeStatus.ACTIVE.value, symbol=None, first_only=True)

                if not active_trades:
                    logging.info(
                        "No active trades found. Defaulting to opening a new trade."
                    )
                    action = ActionType.OPEN_TRADE.value
                else:
                    trade_to_close = random.choice(active_trades)
                    tradde = db.query(Trade).filter_by(trade_id=trade_to_close.trade_id).first()
                    logging.info(f"Found trade. Selected: {trade_to_close}")
                    logging.info(f"Found trade. Selected ID: {trade_to_close.trade_id}")

                    logging.info(f"Found tradde: {tradde}")

                    payload = {
                        "action_type": ActionType.CLOSE_TRADE.value,
                        "client_request_id": generate_request_id(),
                        "trade_id": str(trade_to_close.trade_id),
                        "close_price": round(
                            float(trade_to_close.trade_price) * random.uniform(0.98, 1.02),
                            4,
                        ),
                        # TODO what other reasons could it be?
                        "symbol": trade_to_close.symbol,
                        "close_reason": "RANDOM_TRADE_OUT",
                    }
                    logging.info(f"Success! Generated JSON for closing: {payload}")
                    return payload

            if action == ActionType.OPEN_TRADE.value:
                logging.info(
                    "ASKING BOOKS-SERVICE for available books and market-data-service for symbols..."
                )

                response = fetch_json("http://books-service:8004/books")
                symbols_response = fetch_json("http://market-data-service:8001/symbols")

                books_data = response.get("books", []) if response else []
                symbols_data = symbols_response if symbols_response else {}

                logging.info(
                    f"Fetched {len(books_data)} books and {len(symbols_data)} asset classes."
                )

                if not books_data:
                    logging.error(
                        "Operation aborted: API returned 0 books. Ensure books are created!"
                    )
                    return None

                book = random.choice(books_data)
                book_asset_class = book.get("expected_asset_class")
                logging.info(
                    f"Fetched book {book.get('book_id')} with asset class {book_asset_class}."
                )

                available_symbols = symbols_data.get(book_asset_class, [])

                if not available_symbols:
                    logging.error(
                        f"Operation aborted: Market Data has no symbols for asset class {book_asset_class}."
                    )
                    return None

                symbol = random.choice(available_symbols)
                logging.info(f"Generated symbol: {symbol}")

                payload = {
                    "action_type": ActionType.OPEN_TRADE.value,
                    "client_request_id": generate_request_id(),
                    "book_id": book.get("book_id"),
                    "asset_class": book_asset_class,
                    "symbol": symbol,
                    "side": random.choice(["BUY", "SELL"]),
                    "quantity": random.randint(1, 100) * 10,
                    "trade_price": round(random.uniform(50.0, 150.0), 4),
                    "currency": "USD",
                }
                logging.info(f"Success! Generated JSON for opening: {payload}")
                return payload

        except Exception as e:
            logging.error(f"CRITICAL ERROR in intention generator: {e}", exc_info=True)
            return None
