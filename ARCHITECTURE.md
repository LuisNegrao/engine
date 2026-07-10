# Trading Engine — Components & Architecture

High-level component breakdown for the multi-asset trading engine (stocks, futures, commodities, crypto), supporting short/medium/long-term strategies. Technology choices are deliberately left out — to be discussed separately.

## Components

### 1. Data Feeds
- Exchange/venue adapters producing tick data (and derived bars) from different markets.
- Publishes normalized market data events onto the message bus.
- Strategies (and other components) subscribe only to the instruments/streams relevant to them.
- Also archives everything it receives into the Historical Data Store.

### 2. News / Sentiment Analysis
- Its own module, sibling of the data feeds (not part of them).
- Ingests news and produces structured signal events (e.g. per-instrument sentiment scores).
- Publishes onto the same bus, in the same event style as market data — from a strategy's point of view, a sentiment signal is just another subscribable stream.

### 3. Message Bus
- The backbone connecting all components: ticks, bars, signals, order intents, fills, metrics, control commands.
- Enables the pub/sub model: components are decoupled and only consume what they subscribe to.

### 4. Historical Data Store
- Stores captured tick/bar history (and purchased/imported datasets).
- Replays data through the bus to drive backtests — the same feed interface as live data.

### 5. Strategy Runtime
- Where strategies are coded and run (1..N strategies, isolated from each other so one crashing doesn't take down the engine).
- Event-driven: consumes events (ticks, bars, signals, fills), emits order *intents* — it does not talk to exchanges directly.
- **Single code path for backtesting, paper trading, and live trading.** A strategy cannot tell whether events come from a historical replay, a simulated fill engine, or a live exchange. This is the core architectural rule of the system.
- Emits real-time metrics onto the bus.

### 6. Risk Manager
- Sits between strategies and the OMS.
- Pre-trade checks: position limits, max order size, fat-finger checks.
- Portfolio-level limits: max drawdown, exposure per asset class.
- Global kill switch.

### 7. OMS / Execution Gateway
- Receives validated order intents, routes them to the right broker/exchange adapter.
- Tracks full order state (acknowledged, partial fill, rejected, cancelled) and publishes fills back onto the bus.
- Two interchangeable back-ends behind the same interface:
  - **Live gateway** — real brokers/exchanges.
  - **Simulated fill engine** — used for paper trading and backtesting. Nothing else in the system changes between modes.
- Handles clean strategy termination: stops accepting a killed strategy's orders and can flatten its positions.

### 8. Position & P&L Accounting
- Source of truth for current positions and P&L, fed by fills.
- Reconciles internal state against what brokers/exchanges report.
- Real-time performance metrics live here (not inside each strategy).

### 9. UI
- Dashboards: live strategy performance, P&L, positions, backtest results.
- Control plane: start/stop/terminate strategies, launch backtests.
- Termination must work even when a strategy is misbehaving (routed through the OMS/Risk Manager, not the strategy itself).

### 10. State Persistence & Recovery
- Cross-cutting concern: the engine will crash or need restarts mid-position.
- On startup: reload open positions, working orders, and strategy state.

## Architecture Diagram

```
                        ┌──────────────────────────────────────────────┐
                        │                  MESSAGE BUS                 │
                        │        (ticks, bars, signals, orders,        │
                        │            fills, metrics, commands)         │
                        └──────────────────────────────────────────────┘
                           ▲        ▲         ▲  │          ▲  │
        publishes ticks    │        │ signals │  │ events   │  │ orders
┌────────────────────┐     │        │         │  ▼          │  ▼
│    DATA FEEDS      │─────┘  ┌───────────┐  ┌──────────────────────────┐
│ exchange adapters  │        │   NEWS /  │  │    STRATEGY RUNTIME      │
│ (crypto, stocks,   │        │ SENTIMENT │  │  strategy 1..N (isolated │
│  futures...)       │        │  ANALYSIS │  │  processes), same code   │
└─────────┬──────────┘        └───────────┘  │  for backtest/paper/live │
          │ archives                         └────────────┬─────────────┘
          ▼                                               │ order intents
┌────────────────────┐                                    ▼
│  HISTORICAL DATA   │                       ┌──────────────────────────┐
│  STORE (ticks/bars)│──── replays feed ───▶ │      RISK MANAGER        │
│                    │     into backtests    │ pre-trade checks, limits,│
└────────────────────┘                       │ global kill switch       │
                                             └────────────┬─────────────┘
┌────────────────────┐                                    ▼
│        UI          │                       ┌──────────────────────────┐
│ dashboards, P&L,   │◀── metrics/state ──── │   OMS / EXECUTION        │
│ backtest results,  │─── kill commands ──▶  │ live gateway OR simulated│
│ start/stop strats  │                       │ fill engine (paper/BT)   │
└────────────────────┘                       └────────────┬─────────────┘
                                                          ▼
┌────────────────────┐                       ┌──────────────────────────┐
│ POSITION & P&L     │◀────── fills ──────── │   BROKERS / EXCHANGES    │
│ source of truth,   │                       └──────────────────────────┘
│ reconciliation     │
└────────────────────┘
```

Key property: backtest mode replays the Historical Data Store through the same bus, and the Simulated Fill Engine replaces the Live Gateway. Nothing else in the system changes between backtest, paper, and live.

## Scope note

Start with a single asset class (keeping the instrument model generic so others slot in later), then extend to additional markets once the pipeline works end-to-end.

## Technology Choices

Chosen language: **Java**. Rationale for each pick is recorded so we can revisit decisions later with the original reasoning in hand.

### Language: Java
- Chosen for fluency first — for a project this size, being highly comfortable in the language beats marginal ecosystem advantages.
- Independently a strong fit for trading: much of the institutional trading world runs on the JVM, Interactive Brokers' primary API is Java, and QuickFIX/J is the reference FIX implementation.
- Strong typing catches order/position/instrument bugs at compile time — valuable in a system that moves money.
- High performance ceiling if ever needed (LMAX Disruptor, Chronicle Queue, Aeron all come from trading firms); irrelevant for medium/long-term strategies today, but the headroom is there.
- Known trade-off: research/data-analysis ergonomics are weaker than Python (no pandas equivalent). Mitigation: engine stays in Java; ad-hoc analysis of backtest results can be done in notebooks reading from the same database if ever needed.

### Data Feeds & Broker Connectivity
- **Interactive Brokers (TWS API)** — the one broker covering stocks, futures, commodities and options in a single account, and its official API is Java-first. Primary candidate for non-crypto markets.
- **XChange** — Java's equivalent of ccxt: one unified library for market data and trading across 50+ crypto exchanges (Binance, Coinbase, Kraken, ...), with websocket streaming for major venues. Primary candidate for the crypto starting point.
- **Alpaca** (community Java SDK) and **Polygon.io** (official JVM client) — alternatives for US equities data with cheap/free tiers and paper trading.
- **QuickFIX/J** — only if we ever connect to a venue via the FIX protocol; noted here because Java is its home turf.

### Message Bus: Redis Streams (alternative: NATS)
- One dependency gives pub/sub, streams with replay, and state caching — low operational overhead for a single-person project.
- Kafka is the "enterprise-correct" answer (and Java-native), but its operational weight isn't justified at this scale. Revisit if throughput or durability requirements grow.
- Java clients (Lettuce/Jedis for Redis, jnats for NATS) are all first-class.

### Historical Data Store: QuestDB (alternative: TimescaleDB)
- Purpose-built time-series database for exactly this workload (tick/bar ingestion and range queries).
- Written in Java itself, with a Java-native ingestion client — the most natural fit for our stack.
- TimescaleDB is the fallback: it is plain Postgres (JDBC), which wins if we end up wanting general relational features alongside time-series.
- **Parquet files** for large static backtest datasets — fast columnar reads, cheap storage, portable to analysis tools.

### News / Sentiment Analysis
- **News source:** a news API (NewsAPI, Finnhub, or Benzinga — to be selected) delivering headlines/articles over plain HTTP.
- **Classification: LLM-based** sentiment/relevance scoring per instrument. LLM providers ship official Java SDKs, and the output is just another structured event on the bus.

### Strategy Support Libraries
- **ta4j** — technical indicators in Java.
- **Tablesaw** — closest Java dataframe library, for in-engine data manipulation.
- No off-the-shelf backtesting framework: the event-driven engine we are building *is* the backtester (same code path rule), so Python's richer backtesting ecosystem costs us nothing.

### UI
- **Phase 1: Grafana** — reads straight from QuestDB/Redis, gives real-time dashboards (P&L, positions, metrics) essentially for free before any UI code is written.
- **Phase 2: Spring Boot backend (REST + websockets) with a React frontend** — the custom control plane (start/stop/kill strategies, launch backtests, browse results). Spring Boot chosen because it is the most well-trodden Java web stack; websockets for live metric streaming.

### Position & P&L / State Persistence
- Lives in the same storage layer: QuestDB (or Postgres/TimescaleDB) for fills, positions and P&L history; Redis for hot runtime state (working orders, strategy state) enabling crash recovery.
