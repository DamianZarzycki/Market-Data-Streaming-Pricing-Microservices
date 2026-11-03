"""Process singletons assigned during the FastAPI lifespan."""

quote_service = None
curve_service = None
poll_workers = None


def get_quote_service():
    if quote_service is None:
        raise RuntimeError("QuoteService not initialized")
    return quote_service


def get_curve_service():
    if curve_service is None:
        raise RuntimeError("CurveService not initialized")
    return curve_service


def get_poll_workers():
    if poll_workers is None:
        raise RuntimeError("Poll workers not initialized")
    return poll_workers
