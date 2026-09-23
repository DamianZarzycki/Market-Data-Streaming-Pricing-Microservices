# Pokrycie klas aktywów danymi providerów

Notatka do nowego czatu. Opisuje, skąd bilet „Generate trade” bierze symbole i cenę, oraz czego z obecnych providerów nie da się wyciągnąć.

## Skąd bierze się mapa

`GET /market-data/symbols` na `market-data-service-integration` zwraca mapę z env plus `FUTURES_CONTRACTS`. Czyta ją `read_symbol_map()` w `core/polling/config.py`. Bilet pokazuje symbole klasy książki. Wyjątek: opcja europejska wybiera underlying z listy EQUITY.

Poll kwotowy i SSE `GET /market-data/stream` biorą equity. Przy zapisie kwotowania equity dopisywane są bliźniacze wiersze FUTURES. FX, BOND i IRS nie idą tym strumieniem.

## Co jest podłączone

| Klasa | Symbole (domyślnie) | Skąd cena | Endpoint |
| --- | --- | --- | --- |
| EQUITY | `MARKET_DATA_SYMBOLS` minus pary FX: AAPL, MSFT, TSLA, GOOGL, AMZN, NVDA, SPY | Alpha Vantage, Finnhub, Twelve Data | SSE + `GET /market-data/quotes` |
| FX | `MARKET_DATA_FX_SYMBOLS`: EURUSD, GBPUSD, USDJPY | Cross z tabeli NBP A (mid w PLN; JPY jest za 100). ECB i FRED nie mają spotu pary | `GET /market-data/fx-quotes` (REST, próg LIVE 36 h) |
| BOND | `MARKET_DATA_BOND_SYMBOLS`: USD_5Y, USD_10Y, EUR_10Y | Tenor krzywej: USD z FRED, EUR z ECB. `last` = stopa | `GET /market-data/curve-quotes` |
| IRS | `MARKET_DATA_IRS_SYMBOLS`: USD_5Y_IRS, USD_10Y_IRS, EUR_10Y_IRS | Ta sama stopa co obligacja o tym tenorze. Nazwa: `{waluta}_{tenor}_IRS` | `GET /market-data/curve-quotes` |
| OPTION | symbole EQUITY (AAPL, SPY, …) jako underlying | Spot z kwotowania akcji. Strike, vol, tenor i call/put wpisuje bilet. Premia to Black-Scholes (r = 0) | SSE + `GET /market-data/quotes` na underlyingu |
| FUTURES | `MARKET_DATA_FUTURES_CONTRACTS`: ES→SPY×50, MES→SPY×5, NQ→QQQ×20 | Ta sama cena co ETF. Zapis equity dopisuje bliźniaczy wiersz `asset_class=FUTURES` | SSE + `GET /market-data/quotes` |

Po cyklu krzywych wiersze BOND i IRS są zapisywane do `provider_quotes` (`persist_quote`), bo zapis transakcji szuka tam notowania.

Env i compose: `backend/env.example`, `backend/docker-compose.yml`. Po zmianie zmiennych trzeba przebudować serwis integracji.

## Czego providerzy nie oddają

Podłączone odczyty:

- Alpha Vantage, Finnhub, Twelve Data: tylko kwotowanie akcji (`GLOBAL_QUOTE` / `quote`). Zapisują `asset_class: EQUITY`.
- NBP: tabela FX spot (PLN).
- FRED: rentowności USD (`1M`, `3M`, `1Y`, `5Y`, `10Y`, `30Y`).
- ECB: krzywa EUR (`3M`, `1Y`, `2Y`, `5Y`, `10Y`, `30Y`).

### OPTION

Żaden provider nie zwraca łańcucha (strike, volatility, right, maturity). Bilet książki OPTION wybiera underlying z listy EQUITY i bierze jego spot. Strike, zmienność, zapadalność i call/put są na formularzu. Premia wejścia i późniejsza wycena to europejski Black-Scholes przy r = 0. Symulator `market-data-service` nadal emituje `ACME` osobno; bilet go nie używa.

### FUTURES

Żaden provider nie zwraca ceny futures. Katalog `MARKET_DATA_FUTURES_CONTRACTS` (`SYMBOL:UNDERLYING:MULTIPLIER`) wskazuje ETF, który już pollujemy. Przy zapisie kwotowania equity powstaje bliźniaczy wiersz z symbolem kontraktu i `asset_class=FUTURES`, więc bilet i sprawdzenie świeżości mają co znaleźć. Wycena to ta cena razy mnożnik razy ilość. To nie jest notowanie z giełdy futures.

To samo dotyczy COMMODITY i BENCHMARK: enum je zna, providerzy ich nie dostarczają.
