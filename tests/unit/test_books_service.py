import asyncio
import sys
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from service_loader import load_service
from shared.trading_shared.enums import EventType
from shared.trading_shared.http_errors import MAX_BODY_BYTES, MaxBodySizeMiddleware


@contextmanager
def _client(session=None):
    app_module = load_service("books-service")
    stack = ExitStack()
    stack.enter_context(patch.object(app_module, "start_retention"))
    stack.enter_context(patch.object(app_module, "stop_retention"))
    stack.enter_context(patch.object(app_module.AuditLogger, "ainfo", new_callable=AsyncMock))
    warning = stack.enter_context(
        patch.object(app_module.AuditLogger, "awarning", new_callable=AsyncMock)
    )
    error = stack.enter_context(
        patch.object(app_module.AuditLogger, "aerror", new_callable=AsyncMock)
    )
    session_local = None
    if session is not None:
        session_local = stack.enter_context(
            patch(
                "shared.trading_shared.db._async_session_factory",
                return_value=lambda: session,
            )
        )
    client = stack.enter_context(
        TestClient(app_module.app, raise_server_exceptions=False)
    )
    try:
        yield client, sys.modules["books.manager"], None, warning, error, session_local
    finally:
        stack.close()


def _session():
    db = AsyncMock()
    # AsyncSession.add only stages a row. It is not awaited.
    db.add = MagicMock()
    db.in_transaction = MagicMock(return_value=True)
    return db


def _execute_result(**methods):
    result = MagicMock()
    for name, value in methods.items():
        getattr(result, name).return_value = value
    return result


def _manager():
    load_service("books-service")
    return sys.modules["books.manager"]


def test_get_books_commits_and_closes_the_session():
    db = _session()
    row = SimpleNamespace(
        book_id=uuid4(),
        name="Equity",
        description="desk",
        expected_asset_class="EQUITY",
        is_active=True,
    )
    db.execute.return_value = _execute_result(all=[row])
    with _client(db) as (client, _, _, _, error, _):
        response = client.get("/books")
    assert response.status_code == 200
    assert response.json() == {
        "books": [
            {
                "book_id": str(row.book_id),
                "name": "Equity",
                "description": "desk",
                "expected_asset_class": "EQUITY",
                "is_active": True,
            }
        ]
    }
    db.commit.assert_called_once()
    db.rollback.assert_not_called()
    db.close.assert_called_once()
    error.assert_not_called()


def test_get_books_rolls_back_and_closes_when_the_query_fails():
    db = _session()
    db.execute.side_effect = RuntimeError("db down")
    with _client(db) as (client, _, _, warning, error, _):
        response = client.get("/books")
    assert response.status_code == 500
    assert response.json() == {"error": "Internal server error"}
    db.rollback.assert_called_once()
    db.commit.assert_not_called()
    db.close.assert_called_once()
    warning.assert_not_called()
    error.assert_not_called()


def test_health_does_not_open_a_session():
    db = _session()
    with _client(db) as (client, _, _, _, _, session_local):
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "UP"
    assert response.json()["service"] == "books-service"
    session_local.assert_not_called()


def test_get_book_queries_only_the_response_columns():
    manager = _manager()
    db = AsyncMock()
    db.execute.return_value = _execute_result(all=[])
    with patch.object(manager.logging, "debug") as debug:
        assert asyncio.run(manager.get_all_books(db)) == []
    statement = db.execute.await_args.args[0]
    assert [column.key for column in statement.selected_columns] == [
        "book_id",
        "name",
        "description",
        "expected_asset_class",
        "is_active",
    ]
    assert all("%s" in call.args[0] for call in debug.call_args_list)
    debug.assert_called()


def test_get_book_commits_and_closes_the_session():
    book_id = uuid4()
    db = _session()
    db.execute.return_value = _execute_result(
        first=SimpleNamespace(
            book_id=book_id,
            name="Equity",
            description=None,
            expected_asset_class="EQUITY",
            is_active=True,
        )
    )
    with _client(db) as (client, _, _, _, error, _):
        response = client.get(f"/books/{book_id}")
    assert response.status_code == 200
    assert response.json()["book_id"] == str(book_id)
    assert response.json()["description"] is None
    db.commit.assert_called_once()
    db.rollback.assert_not_called()
    db.close.assert_called_once()
    error.assert_not_called()


def test_missing_book_is_404():
    book_id = uuid4()
    db = _session()
    db.execute.return_value = _execute_result(first=None)
    with _client(db) as (client, _, _, _, error, _):
        response = client.get(f"/books/{book_id}")
    assert response.status_code == 404
    assert response.json() == {"error": "Book not found"}
    db.rollback.assert_called_once()
    db.commit.assert_not_called()
    db.close.assert_called_once()
    error.assert_not_called()


def test_non_uuid_book_id_is_400():
    db = _session()
    with _client(db) as (client, _, _, warning, error, session_local):
        response = client.get("/books/not-a-uuid")
    assert response.status_code == 400
    assert response.json() == {"error": "Invalid book id"}
    warning.assert_not_called()
    error.assert_not_called()
    session_local.assert_not_called()


def test_asset_class_change_does_not_count_trades():
    manager = _manager()
    book = MagicMock()
    db = AsyncMock()
    found = MagicMock()
    found.scalars.return_value.first.return_value = book
    db.execute.return_value = found
    with patch.object(manager, "count_active_trades") as count, patch.object(manager, "append_audit") as append:
        updated = asyncio.run(
            manager.update_book(
                db, "book-1", {"expected_asset_class": "FX"}, updated_by="API"
            )
        )
    count.assert_not_called()
    assert book.expected_asset_class == "FX"
    assert updated == {"message": "Book with id book-1 updated successfully"}
    db.commit.assert_not_called()
    db.flush.assert_called_once()
    assert append.call_args.args[0] is db
    assert append.call_args.args[1] is EventType.DB_UPDATE


def test_create_stages_db_create_on_the_request_session():
    manager = _manager()
    db = _session()
    with patch.object(manager, "append_audit") as append:
        asyncio.run(
            manager.create_books_batch(
                db,
                [{"name": "Equity", "expected_asset_class": "EQUITY"}],
                created_by="API",
            )
        )
    db.commit.assert_not_called()
    db.flush.assert_called_once()
    assert append.call_args.args[0] is db
    assert append.call_args.args[1] is EventType.DB_CREATE
    assert append.call_args.kwargs["payload"]["performed_by"] == "API"


def test_duplicate_name_in_batch_commits_only_the_reject_audit():
    manager = _manager()
    db = _session()
    book = {"name": "Equity", "expected_asset_class": "EQUITY"}
    with patch.object(manager, "append_audit") as append:
        with pytest.raises(manager.ApiError) as exc:
            asyncio.run(manager.create_books_batch(db, [book, dict(book)]))
    assert exc.value.status_code == 409
    assert "Duplicate book name" in exc.value.message
    db.rollback.assert_called_once()
    db.commit.assert_called_once()
    assert append.call_args.args[1] is EventType.DB_REJECT
    assert append.call_args.kwargs["severity"].value == "WARNING"


def test_unique_constraint_commits_only_the_reject_audit():
    manager = _manager()
    db = _session()
    db.flush.side_effect = IntegrityError("insert", {}, Exception("unique"))
    with patch.object(manager, "append_audit") as append:
        with pytest.raises(manager.ApiError) as exc:
            asyncio.run(
                manager.create_books_batch(
                    db, [{"name": "Equity", "expected_asset_class": "EQUITY"}]
                )
            )
    assert "already exists" in exc.value.message
    db.rollback.assert_called_once()
    db.commit.assert_called_once()
    assert append.call_args.args[1] is EventType.DB_REJECT


def test_delete_with_active_trades_commits_only_the_reject_audit():
    manager = _manager()
    book = MagicMock()
    db = AsyncMock()
    found = MagicMock()
    found.scalars.return_value.first.return_value = book
    db.execute.return_value = found
    with (
        patch.object(manager, "count_active_trades", new_callable=AsyncMock, return_value=4),
        patch.object(manager, "append_audit") as append,
    ):
        with pytest.raises(manager.ApiError) as exc:
            asyncio.run(manager.delete_book(db, "book-1"))
    assert exc.value.status_code == 409
    assert "4 active trade" in exc.value.message
    assert book.is_active is not False
    db.commit.assert_called_once()
    assert append.call_args.args[1] is EventType.DB_REJECT
    assert append.call_args.kwargs["entity_id"] == "book-1"
    assert append.call_args.kwargs["payload"]["active_trades"] == 4


def test_delete_stages_db_delete_on_the_request_session():
    manager = _manager()
    book = MagicMock()
    db = AsyncMock()
    found = MagicMock()
    found.scalars.return_value.first.return_value = book
    db.execute.return_value = found
    with (
        patch.object(manager, "count_active_trades", new_callable=AsyncMock, return_value=0),
        patch.object(manager, "append_audit") as append,
    ):
        result = asyncio.run(manager.delete_book(db, "book-1"))
    assert result == {"message": "Book with id book-1 marked as inactive"}
    assert book.is_active is False
    db.commit.assert_not_called()
    assert append.call_args.args[0] is db
    assert append.call_args.args[1] is EventType.DB_DELETE


def test_create_failure_rolls_back_and_audits_db_error_on_its_own_session():
    db = _session()
    with _client(db) as (client, manager, _, _, error, _):
        with patch.object(
            manager,
            "create_books_batch",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            response = client.post(
                "/books",
                json={"name": "Equity", "expected_asset_class": "EQUITY"},
            )
    assert response.status_code == 500
    assert response.json() == {"error": "Internal server error"}
    db.rollback.assert_called_once()
    db.commit.assert_not_called()
    db.close.assert_called_once()
    assert error.call_args.args[0] is EventType.DB_ERROR
    assert error.call_args.kwargs["payload"]["error_type"] == "RuntimeError"
    assert error.call_args.kwargs["entity_id"] == ""


def test_create_accepts_one_object_or_a_list():
    db = _session()
    seen = []

    async def capture(_db, books, created_by=None):
        seen.append((len(books), created_by))
        return ["id-1"]

    with _client(db) as (client, manager, _, _, _, _):
        with patch.object(manager, "create_books_batch", side_effect=capture):
            one = client.post(
                "/books",
                json={"name": "Equity", "expected_asset_class": "EQUITY"},
            )
            many = client.post(
                "/books",
                json=[
                    {"name": "Equity", "expected_asset_class": "EQUITY"},
                    {"name": "FX", "expected_asset_class": "FX", "description": "spot"},
                ],
            )
    assert one.status_code == 201
    assert one.json() == {"message": "Books created successfully", "book_ids": ["id-1"]}
    assert many.status_code == 201
    assert seen == [(1, "API"), (2, "API")]


def test_empty_payload_is_400_and_missing_fields_are_409():
    db = _session()
    with _client(db) as (client, _, _, warning, error, _):
        empty_object = client.post("/books", json={})
        empty_list = client.post("/books", json=[])
        invalid = client.post(
            "/books",
            content=b"{",
            headers={"content-type": "application/json"},
        )
        missing = client.post("/books", json={"description": "x"})
        blank = client.post(
            "/books",
            json={"name": "", "expected_asset_class": "EQUITY"},
        )
    assert empty_object.status_code == 400
    assert empty_object.json() == {"error": "Invalid JSON payload"}
    assert empty_list.status_code == 400
    assert empty_list.json() == {"error": "Invalid JSON payload"}
    assert invalid.status_code == 400
    assert invalid.json() == {"error": "Invalid JSON payload"}
    assert missing.status_code == 409
    assert missing.json() == {"error": "Missing required fields (name, expected_asset_class)"}
    assert blank.status_code == 409
    assert blank.json() == {"error": "Missing required fields (name, expected_asset_class)"}
    assert warning.call_count == 2
    assert all(call.args[0] is EventType.DB_REJECT for call in warning.call_args_list)
    error.assert_not_called()


def test_put_and_delete_keep_their_message_bodies():
    book_id = uuid4()
    db = _session()
    with _client(db) as (client, manager, _, _, _, _):
        with (
            patch.object(
                manager,
                "update_book",
                new_callable=AsyncMock,
                return_value={"message": f"Book with id {book_id} updated successfully"},
            ),
            patch.object(
                manager,
                "delete_book",
                new_callable=AsyncMock,
                return_value={"message": f"Book with id {book_id} marked as inactive"},
            ),
        ):
            updated = client.put(f"/books/{book_id}", json={"name": "Rates"})
            deleted = client.delete(f"/books/{book_id}")
    assert updated.status_code == 200
    assert updated.json()["message"] == f"Book with id {book_id} updated successfully"
    assert deleted.status_code == 200
    assert "inactive" in deleted.json()["message"]


def test_request_body_over_the_cap_is_rejected_before_the_session():
    db = _session()
    with _client(db) as (client, _, _, warning, error, session_local):
        response = client.post(
            "/books",
            content=b"x" * (MAX_BODY_BYTES + 1),
            headers={"content-type": "application/json"},
        )
    assert response.status_code == 413
    assert response.json() == {"error": "Request body is too large"}
    session_local.assert_not_called()
    warning.assert_not_called()
    error.assert_not_called()


def test_declared_content_length_over_the_cap_is_not_read():
    async def scenario():
        called = []

        async def app(scope, receive, send):
            called.append(True)

        async def receive():
            raise AssertionError("body was read")

        sent = []

        async def send(message):
            sent.append(message)

        middleware = MaxBodySizeMiddleware(app, max_bytes=4)
        await middleware(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-length", b"100")],
            },
            receive,
            send,
        )
        assert called == []
        assert sent[0]["status"] == 413
        assert sent[1]["body"] == b'{"error": "Request body is too large"}'

    asyncio.run(scenario())


def test_chunked_body_over_the_cap_is_rejected():
    async def scenario():
        called = []

        async def app(scope, receive, send):
            called.append(await receive())

        chunks = [
            {"type": "http.request", "body": b"abcd", "more_body": True},
            {"type": "http.request", "body": b"ef", "more_body": False},
        ]

        async def receive():
            return chunks.pop(0)

        sent = []

        async def send(message):
            sent.append(message)

        middleware = MaxBodySizeMiddleware(app, max_bytes=5)
        await middleware(
            {"type": "http", "method": "POST", "headers": []},
            receive,
            send,
        )
        assert called == []
        assert sent[0]["status"] == 413

    asyncio.run(scenario())


def test_body_within_the_cap_is_replayed_to_the_app():
    async def scenario():
        seen = {}

        async def app(scope, receive, send):
            seen["message"] = await receive()

        async def receive():
            return {"type": "http.request", "body": b"{}", "more_body": False}

        async def send(message):
            return None

        middleware = MaxBodySizeMiddleware(app, max_bytes=8)
        await middleware(
            {"type": "http", "method": "PUT", "headers": [(b"content-length", b"2")]},
            receive,
            send,
        )
        assert seen["message"]["body"] == b"{}"
        assert seen["message"]["more_body"] is False

    asyncio.run(scenario())


def test_service_start_and_stop_are_audited():
    app_module = load_service("books-service")
    order = []

    async def record(self, event_type, *args, **kwargs):
        order.append(event_type)

    with (
        patch.object(app_module, "start_retention", side_effect=lambda: order.append("start")),
        patch.object(app_module, "stop_retention", side_effect=lambda: order.append("stop")),
        patch.object(app_module.AuditLogger, "ainfo", record),
    ):
        with TestClient(app_module.app):
            pass
    assert order == [
        EventType.WORKER_STARTED,
        "start",
        "stop",
        EventType.WORKER_STOPPED,
    ]
