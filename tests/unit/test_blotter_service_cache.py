import sys

import pytest

from service_loader import load_service


@pytest.fixture(scope="module")
def cache():
    load_service("blotter-service")
    return sys.modules["cache.valuation_cache_service"]


@pytest.fixture(autouse=True)
def _reset(cache):
    cache.valuation_cache.clear()
    yield


def test_update_and_lookup_history_per_trade(cache):
    service = cache.ValuationCacheService()
    service.update_cache("t1", {"fair_value": 1})
    service.update_cache("t1", {"fair_value": 2})
    service.update_cache("t2", {"fair_value": 9})

    assert service.get_valuation_from_cache("t1") == [
        {"fair_value": 1},
        {"fair_value": 2},
    ]
    assert service.get_valuation_from_cache("missing") == []
    assert service.get_latest_valuation("t1") == {"fair_value": 2}
    assert service.get_latest_valuation("missing") is None


def test_history_keeps_the_newest_twenty(cache):
    service = cache.ValuationCacheService()
    for value in range(21):
        service.update_cache("t1", {"n": value})

    history = service.get_valuation_from_cache("t1")
    assert len(history) == cache.MAX_VALUATIONS_PER_TRADE == 20
    assert [item["n"] for item in history] == list(range(1, 21))
    assert service.get_latest_valuation("t1") == {"n": 20}


def test_clear_cache_empties_the_shared_store(cache):
    service = cache.ValuationCacheService()
    service.update_cache("t1", {"fair_value": 1})

    service.clear_cache()

    assert cache.valuation_cache == {}
    assert service.get_latest_valuation("t1") is None
