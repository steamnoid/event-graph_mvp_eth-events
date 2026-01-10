# instructions.md — Event Graph MVP

## Purpose

Implement a read-only, post-factum event graph system for Uniswap V3 (WETH/USDC),
focused on causality, invariants, and behavioral reasoning.

## Conceptual foundations

Mandatory reading:
1. https://medium.com/@picospitfire/when-flaky-tests-stop-being-a-qa-problem-c66cf2e346cd
2. https://medium.com/@picospitfire/eventdrivenarchitecture-part-2-4-event-graphs-in-event-driven-systems-47347a2574d6
3. https://medium.com/@picospitfire/eventdrivenarchitecture-part-3-4-event-graphs-as-a-decoupling-layer-1210fab3ef90
4. https://medium.com/@picospitfire/eventdrivenarchitecture-part-4-4-how-event-graphs-change-the-role-of-qa-and-test-automation-479a9947f29b

## Event identity

event_id = tx_hash + ":" + log_index

## Causality rules

- Parents must come from the same transaction
- Parent logIndex must be lower
- Resulting graph must be a DAG

### Transfer IN
- No parents

### Swap / Mint
- Parents = all Transfer IN earlier in the transaction

### Transfer OUT
- Parent = last pool state-changing event

### Burn
- Parent = last Mint or Burn (simplified)

## Tech stack

- Apache Airflow
- Python
- web3.py
- Neo4j
- Cypher

No AI/ML. No E2E tests. No timing-based assertions.
