import asyncio
from abc import ABC, abstractmethod

from core.runtime import client


class ProviderAPIError(Exception):
    def __init__(self, message: str, status: str = "error"):
        super().__init__(message)
        self.status = status


class BaseProvider(ABC):
    key: str
    semaphore_limit: int = 1
    url: str
    # True when one HTTP call can return quotes for every symbol.
    batch_is_single_request: bool = False

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(self.semaphore_limit)

    @abstractmethod
    def normalize_data(self, data, symbol) -> dict:
        pass

    @abstractmethod
    async def fetch_data(self, symbol: str):
        pass

    @abstractmethod
    def health(self) -> ProviderAPIError | None:
        pass

    # Async: awaits AsyncClient.get.
    async def get_json(self, url: str, headers: dict | None = None):
        response = await client().get(url, headers=headers)
        response.raise_for_status()
        return response.json()
