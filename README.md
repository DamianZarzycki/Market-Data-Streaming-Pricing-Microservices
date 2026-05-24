# Market Data & Pricing System

## 1. System Overview
A distributed system that simulates real-time stock market data and prices financial instruments based on it. The application consists of three independent microservices that talk to each other in a containerized environment, ensuring background processing and system monitoring.

## 2. System Architecture
The system is based on 3 services:
* **market-data-service (Port: 8001):** Simulates the stock market. It has a background task that constantly creates new price ticks for stocks (EQUITY), bonds (BOND), and currencies (FX).
* **pricing-service (Port: 8002):** Takes data from the market through an open connection, calculates instrument prices (fair value, present value, forward) on the fly, and saves them in memory.
* **monitoring-service (Port: 8003):** Checks if the services are working. It runs independently, regularly asking other services for their status, and provides a combined health report for the whole system.

## 3. Running the Project
In the main project folder, run this command:
```bash
docker compose up --build
```

## 4. Endpoints Description

### Market Data Service (localhost:8001)
* **GET /health** - Returns the service status and stats about generated events.
* **GET /snapshot** - Returns the current state of all market instruments.
* **GET /stream** - Opens a streaming connection with generated events.

### Pricing Service (localhost:8002)
* **GET /health** - Returns the service status, market stream connection state, and the time of the last pricing.
* **GET /valuations** - Returns all currently calculated instrument prices.
* **GET /valuations/<instrument_id>** - Returns the price of a specific instrument (e.g., EQ_ACME).

### Monitoring Service (localhost:8003)
* **GET /health** - Returns the status of the monitoring service itself.
* **GET /status** - Returns a full report on the uptime (UP/DOWN) and response times of the other services.

## 5. Streaming Mechanism
We used SSE (Server-Sent Events). It is a one-way connection from the server to the client. The client connects to the server, and the server keeps sending new events in an endless loop.

## 6. Concurrency in the System
* **Background workers** - each service has a separate task running in the background.
* **ThreadedServer** - a multi-threaded server stops the whole system from getting blocked.
* **Locks** - give us access to safe and consistent data.

## 7. Testing the System
You can test the system using `curl` commands or by opening the links in a web browser.

## 8. Implementation Challenges
* The biggest architectural challenge was handling concurrency, especially using locks (`threading.Lock`):

* Putting an unnecessary lock on network tasks (like asking other services via HTTP) slowed down the whole system or blocked it completely.