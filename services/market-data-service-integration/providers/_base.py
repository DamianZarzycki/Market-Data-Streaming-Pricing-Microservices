from abc import ABC, abstractmethod
import urllib.request
import json


class BaseProvider(ABC):
    def __init__(self, api_key: str):
        self.api_key = api_key

    # @abstractmethod
    # def normalize_data(self, data):
    #     pass

    @abstractmethod
    def normalize_data(self, data, symbol) -> dict:
        pass

    @abstractmethod
    def fetch_data(self, symbol: str):
        pass

    @abstractmethod
    def health(self):
        pass