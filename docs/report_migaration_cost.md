# Koszt przejścia na FastAPI

Godziny są dla jednej osoby, która zna te serwisy i nie ma tu jeszcze FastAPI na produkcji. To czas pracy przy kodzie, nie czas kalendarzowy. Baza zostaje na dzisiejszym, synchronicznym SQLAlchemy. Pętla zdarzeń jest tylko tam, gdzie wisi strumień. Tę decyzję podjął benchmark: przy 700 jednoczesnych zapisach FastAPI był około dwa razy szybszy, ale i tak nie wszedł w pasek 1200/s, a ogon odpowiedzi został przy 2 sekundach.

Stary kosztorys w [risk_matrix.md](risk_matrix.md) ma środek około 125 h i próg 140 h. Liczył samo przepisanie tras. Poniżej jest wersja realistyczna: do każdej usługi dochodzą testy kontraktu, przeniesienie wątków z `if __name__ == "__main__"` oraz sprawdzenie, że blokujące wywołanie nie weszło na pętlę. Przy ośmiu serwisach wychodzi **150–220 h**. Przy dwóch, które benchmark uzasadnia, **50–70 h**.

## Jak to jest liczone

Na usługę: postawienie uvicorn i `lifespan`, potem trasy, potem każdy strumień osobno, potem każdy wątek tła, na końcu kontrakt. Usługi z testami (pricing 20, blotter 12, market-data 14) przenoszą je. Pięć pozostałych testów jednostkowych nie ma. Jest `backend/tests/curl_tests.sh` na cały stos, ale to dym, nie kontrakt. `backend/tests/curl_contract.sh` pokrywa tylko pricing, market-data i blotter.

Nie wliczam: sprzątania cache 50 pozycji w pricingu, indeksu po symbolu, ani zmiany booksa na serwer wielowątkowy. To praca na Bottle i FastAPI jej nie skraca.

## Raz, niezależnie od liczby usług

| Co | Czas | Po co |
| --- | --- | --- |
| Wzorzec: jeden proces, `lifespan`, błąd 400 z dzisiejszym JSON (`error` / `message` / `accepted_so_far`), nie 422 | 8–12 h | Inaczej każdy serwis powtarza te same pułapki |
| Przepisanie `trading_shared` na async | 16–24 h | Tylko jeśli baza jednak idzie na async. W sumach poniżej tego nie ma |

Nowości wspólne dla całego zespołu: `async def` oddaje pętlę tylko na `await`. Zwykły `urlopen`, `time.sleep`, `queue.Queue.get()` i dzisiejszy `commit()` w takiej funkcji zatrzymują cały proces. Uvicorn nie wykonuje bloku `if __name__ == "__main__"`, więc wątki tła, które dziś startują przed `app.run`, po cichu nie wstają, a `/health` i tak jest zielone. Zostaje jeden proces. Drugi worker dostałby własną pamięć i własną kolejkę.

## Wszystkie usługi

| Usługa | Czas | Czy benchmark to uzasadnia |
| --- | --- | --- |
| market-data | 16–24 h | Tak. `GET /stream` |
| pricing | 24–32 h | Tak. `GET /valuation-stream`. Liczenie wycen zostaje wąskim gardłem i tak |
| blotter | 12–18 h | Nie. Czyta jeden strumień, sam kart nie trzyma |
| monitoring | 8–12 h | Nie. Dwie trasy, sonda w tle |
| books | 12–16 h | Nie. Wystarczy serwer wielowątkowy, 1–2 h |
| trade-action | 14–20 h | Nie. Jeden konsument i blokada wiersza |
| trade-generation | 18–28 h | Nie. Tempo wyznacza interwał, nie liczba kart |
| integration | 40–55 h | Nie. Dwa strumienie wołają dostawców, limity zostają |
| Wzorzec, raz | 8–12 h | Tak, przy pierwszym serwisie |

Dwa uzasadnione serwisy plus wzorzec: **48–68 h**, w zaokrągleniu **50–70 h**. Wszystkie osiem plus wzorzec: **152–217 h**, w zaokrągleniu **150–220 h**.

## market-data, 16–24 h

Cztery trasy, 14 testów, trzy wątki (`market_worker`, `metric_worker`, `db_worker`), strumień `GET /stream`.

Przeszkody. Zapis krzywej do Postgresa idzie na każdym tiku, zanim ramka pójdzie na strumień. Przeniesienie tego zapisu na pętlę zatrzyma karty. Snapshot i lista subskrybentów żyją w pamięci procesu, więc zostaje jeden worker. Odłączenie klienta musi zdjąć go z listy, inaczej fan-out rośnie w nieskończoność.

Nowości. `StreamingResponse` i start wątków w `lifespan`. Zapis do bazy zostaje w wątku, nie w `async def`.

## pricing, 24–32 h

Sześć tras, 20 testów, sześć wątków, strumień `GET /valuation-stream`. Jeden wątek wisi na `urlopen` strumienia market-data, inne co 5 s chodzą po bazie i po integration (limit 5 s).

Przeszkody. To największy zysk ze strumieni i jednocześnie miejsce, w którym łatwo go oddać: wstawienie `urlopen` albo synchronicznego `commit()` do `async def` ubija `/health`, co benchmark pokazał w 5 z 6 przebiegów. Słowniki pod lockiem nie mogą być jednocześnie trzymane z wątku i z pętli. Cache 50 najnowszych transakcji i liczenie na każdy tik FastAPI nie przyspiesza. Blotter zostaje na Bottle i dalej czyta ten strumień.

Nowości. Osobna reguła w tym samym pliku: strumień jest `async`, baza, CPU i `urlopen` zostają w zwykłej funkcji albo w dotychczasowym wątku.

## blotter, 12–18 h

Sześć tras, 12 testów, jeden wątek czytający `valuation-stream` (limit 30 s, potem ponowne łączenie). `GET /books/summary` woła books z limitem 2 s. Reszta to odczyt transakcji z bazy.

Przeszkody. Usługa nie trzyma kart traderów. Przepisanie nie rusza wyniku, który uzasadnia FastAPI. Wątek SSE ma zostać wątkiem. Books jest jednowątkowy, więc podsumowanie i tak stoi za nim.

Nowości. Mało, jeśli odczyty zostaną w zwykłych funkcjach. Kontrakt JSON jest już w `curl_contract.sh`.

## monitoring, 8–12 h

Dwie trasy, brak testów jednostkowych, jeden wątek. Sześć razy `GET /health` po kolei, limit 2 s na każde, potem sekunda snu.

Przeszkody. Brak strumienia. Sonda booksa dokłada żądanie do jedynego wątku booksa. Sześć wiszących sond na pętli zatrzymuje `GET /status`.

Nowości. Żadnej, która by się zwróciła. Sondy zostają w wątku.

## books, 12–16 h

Sześć tras, brak testów, zero wątków tła. Serwer sam w sobie obsługuje jedno żądanie naraz. `POST /books` zamyka wspólną sesję bazy i kolejne żądanie trafia na zamkniętą sesję.

Przeszkody. FastAPI nie jest tu lekarstwem. Serwer wielowątkowy, taki jak w pozostałych usługach, to 1–2 h. Naprawa sesji i tak trzeba zrobić, czy framework się zmieni, czy nie. Wspólna sesja procesu nie przeżyje równoległych żądań.

Nowości. Przy pełnym przepisaniu: kontrakt na 400/404 i body JSON, bo dziś części błędów nie ma czym zamrozić.

## trade-action, 14–20 h

Pięć tras, brak testów jednostkowych, jeden wątek, kolejka 100. Jedyny pisarz `trades`. `POST` zwraca 202, zanim wiersz powstanie. Pełna kolejka to 503, w batchu dodatkowo `accepted_so_far`. `CLOSE` trzyma blokadę wiersza do `commit`.

Przeszkody. `queue.Queue.get()` na pętli zatrzymuje cały proces. Drugi konsument, „skoro FastAPI umie więcej naraz”, rozjedzie `CLOSE` na tej samej transakcji. Po biznesowym `commit` audit i tak otwiera drugie połączenie.

Nowości. Kody 202 i 503 muszą zostać. FastAPI domyślnie zrobi z części błędów 422 i inne body. Kolejka zostaje wątkiem.

## trade-generation, 18–28 h

Dziewięć tras, brak testów jednostkowych. Wątek startuje na `POST /start`, nie przy imporcie. Woła books (2 s), market-data albo integration oraz trade-action (5 s, trzy próby). W tle połyka błąd wysyłki.

Przeszkody. Ciche błędy: zła odpowiedź trade-action wygląda jak wolniejszy generator. Trzy równoległe pobrania (books, symbole, snapshot) są na puli trzech wątków. W trybie dostawcy pętla zwalnia do co najmniej 15 s. Tempo nie zależy od liczby traderów.

Nowości. `POST /start` i `POST /stop` muszą działać tak samo, bo uvicorn nie odpala `__main__`. To akurat jest prostsze niż w innych usługach, bo wątek już startuje z trasy.

## integration, 40–55 h

Szesnaście tras, brak testów, pięć wątków, dwa strumienie (`/market-data/stream` i wariant z dostawcą). Dwa pliki importują Bottle poza handlerami. Poll dostawców woła `urlopen` bez limitu czasu. `GET /market-data/refresh` bierze ten sam lock co poll i nie wraca, aż cykl się skończy. Obraz jest na Pythonie 3.10, reszta na 3.11. Limity dostawców są w bazie i nie ruszają się od zmiany serwera.

Przeszkody. Największa usługa i jedyna, w której framework jest wpleciony poza trasami. Dwie kolejki mają różne zachowanie przy przepełnieniu: krzywe są porzucane, kolejka notowań nie ma sufitu. Strumień na każdym okrążeniu woła dostawców, więc to nie jest tania wisząca karta z testu. Przepisanie bez polla zostawia dwie drogi do tego samego locka.

Nowości. Wszystkie naraz: `lifespan` dla pięciu wątków, dwa strumienie, timeout na pollu (warto go dodać jeszcze na Bottle), jeden obraz Pythona, kontrakt szesnastu tras od zera.

## Kiedy przestać

Pierwszy serwis to market-data, nie books. Books nie uczy strumienia, a właśnie strumień jest powodem przeprowadzki. Jeśli market-data przekroczy górę swojego zakresu razy 1,5, czyli około 36 h, reszta stosu też będzie droższa niż tabela i warto zatrzymać przeprowadzkę. To ta sama reguła, którą macie przy pilocie booksa (10 h, stop przy 15 h), przeniesiona na usługę, która ma sens.
