import json

from bottle import HTTPResponse


def json_error(status: int, message: str) -> HTTPResponse:
    return HTTPResponse(
        status=status,
        body=json.dumps({"error": message}),
        headers={"Content-Type": "application/json"},
    )


def unknown_provider(provider: str) -> HTTPResponse:
    return json_error(404, f"Unknown provider: {provider}")
