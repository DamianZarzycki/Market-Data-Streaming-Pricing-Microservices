import logging
from bottle import Bottle, request, response  # <-- TUTAJ POPRAWA IMPORTU

from shared.trading_shared.db import SessionLocal
from shared.trading_shared.enums import ServiceStatus

import books_manager_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

app = Bottle()

db = SessionLocal()


@app.route("/health")
def health():
    return {
        "service": "books-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/books", method=["GET"])
def books():
    return {"books": books_manager_service.get_all_books(db)}


@app.route("/books/<book_id>", method=["GET"])
def get_book(book_id):
    book = books_manager_service.get_book_by_id(db, book_id)
    if book:
        return book
    else:
        response.status = 404
        return {"error": "Book not found"}


@app.route("/books", method=["POST"])
def create_new_books():
    try:
        payload = request.json
        logging.info("Received request to create new books with payload: %s", payload)

        if not payload:
            response.status = 400
            return {"error": "Invalid JSON payload"}

        if isinstance(payload, dict):
            payload = [payload]

        created_ids = books_manager_service.create_books_batch(
            db, payload, created_by="API"
        )

        response.status = 201
        return {"message": "Books created successfully", "book_ids": created_ids}

    except ValueError as ve:
        db.rollback()
        response.status = 409
        return {"error": str(ve)}
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating books: {e}")
        response.status = 500
        return {"error": "Internal server error"}
    finally:
        db.close()


@app.route("/books/<book_id>", method=["PUT"])
def update_book(book_id):
    payload = request.json
    if not payload:
        response.status = 400
        return {"error": "Invalid JSON payload"}

    book = books_manager_service.update_book(db, book_id, payload, updated_by="API")
    if not book:
        response.status = 404
        return {"error": "Book not found"}

    return {"message": f"Book with id {book_id} updated successfully"}


@app.route("/books/<book_id>", method=["DELETE"])
def delete_book(book_id):
    book = books_manager_service.delete_book(db, book_id)
    return {"message": f"Book with id {book_id} marked as inactive"}


if __name__ == "__main__":
    logging.info("Starting Books service...")

    # monitoring_thread = threading.Thread()
    # monitoring_thread.daemon = True
    # monitoring_thread.start()

    app.run(host="0.0.0.0", port=8004)
