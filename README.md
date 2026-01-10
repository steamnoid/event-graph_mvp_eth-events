# Event Graph MVP — Uniswap V3 (WETH/USDC)

> **Graph-based event analysis for testing, observability, and business intelligence**  
> built on a real, production-grade, event-driven system.

---

## TL;DR

This repository contains an MVP that demonstrates how **event graphs** can be used to:

- understand system behavior without end-to-end tests
- detect flaky behavior as **structural graph patterns**
- define correctness as **graph invariants**
- unify **testing, observability, and BI** on a single artifact

The MVP is built on **Uniswap V3 (WETH/USDC)** events from **Ethereum mainnet**.

No production code is modified.  
No runtime instrumentation is added.  
Everything is reconstructed **post-factum** from public events.

---

## Core idea

Instead of testing execution paths or asserting timing-based outcomes, this project:

- treats each emitted event as an **atomic fact**
- reconstructs **causal event graphs**
- defines correctness as **invariants over graphs**, not step-by-step tests

### Event primitives

Each event is modeled using three primitives:

- `event_id` – unique identity (`txHash + logIndex`)
- `parents_event_id(s)` – explicit causal dependencies
- `event_type` – stable semantic label (what happened)

Applications emit **local causal truth only**.  
Global behavior is reconstructed later, analytically.

---

## Conceptual foundations

This project is directly inspired by the following article series:

1. **When Flaky Tests Stop Being a QA Problem**  
   https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd

2. **Event Graphs in Event-Driven Systems (Part 2/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6

3. **Event Graphs as a Decoupling Layer (Part 3/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90

4. **How Event Graphs Change the Role of QA and Test Automation (Part 4/4)**  
   https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b

---

## Architecture

```
Ethereum (mainnet)
  ↓
web3.py (raw logs)
  ↓
Apache Airflow (ingestion & backfill)
  ↓
Python (normalization & causality)
  ↓
Neo4j (event graph)
  ↓
Cypher invariants & analysis
```

---

## Status

This is an **MVP / research prototype**.
