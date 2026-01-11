# docker_instructions.md — Containerization Rules (MANDATORY)

## 0. Scope

All dependent components of this project must be containerized using Docker.

The goal of containerization is:
- reproducibility
- isolation
- ease of onboarding
- explicit infrastructure as code

This is not a production deployment.
This is not Kubernetes.
This is not cloud-specific.

---

## 1. What MUST be dockerized

The following components must always run in Docker containers:

### Required
- Apache Airflow (scheduler + webserver)
- Neo4j
- PostgreSQL (Airflow metadata DB)

### Optional
- Local Ethereum RPC (Anvil / Geth)
- Development utilities

---

## 2. What MUST NOT be dockerized

- The human developer
- GitHub Copilot
- The IDE

Python execution must happen either:
- inside Airflow containers, or
- inside a dedicated worker container

No mixed host/container execution.

---

## 3. Docker orchestration

Required tool:
- docker-compose

The project must start with:
```bash
docker compose up
```

No Kubernetes.
No Helm.
No Terraform.

---

## 4. Container philosophy

Each container must:
- have a single responsibility
- expose explicit ports
- define explicit volumes
- define explicit environment variables

No hidden state.
No implicit behavior.

---

## 5. Required containers

### 5.1 Apache Airflow
- Image: apache/airflow
- Components: webserver, scheduler
- Uses PostgreSQL as metadata DB
- DAGs mounted as volume
- Logs persisted as volume

### 5.2 PostgreSQL
- Used only for Airflow metadata
- Not an analytics store

### 5.3 Neo4j
- Image: neo4j
- Used as event graph store
- Graph is read-only from application perspective
- Data persisted via volume

---

## 6. Volumes

Minimum required volumes:

```
./dags           → /opt/airflow/dags
./logs           → /opt/airflow/logs
./neo4j/data     → /data
./neo4j/logs     → /logs
```

Volumes must be local and explicit.

---

## 7. Networking

- All containers must share a Docker network
- Services must be addressed by service name

Examples:
- neo4j://neo4j:7687
- postgresql://postgres:5432

---

## 8. Environment configuration

- Configuration via .env files or docker-compose env vars
- No hardcoded secrets
- No credentials in code

---

## 9. Airflow rules

- DAGs must be deterministic
- Retries are allowed and expected
- Retries are infrastructure concerns, not flaky behavior
- DAGs must never mutate historical event data

---

## 10. Neo4j rules

- Minimal schema only:
  - (:Event)
  - [:CAUSES]
- No triggers
- No hidden logic
- APOC only if explicitly approved

---

## 11. Python rules

- Pure functions
- Deterministic behavior
- Idempotent execution
- No side effects at import time

---

## 12. Copilot constraints

Copilot operates in strict agent mode.
It may edit files and run commands/tools autonomously, while respecting the project rules in the instruction set.

---

## 13. Non-goals

This project must not:
- become a platform
- introduce ML pipelines
- introduce dashboards
- introduce runtime graph building

---

## 14. Guiding principles

If a container does more than one thing, it is incorrect.
If a component cannot be explained, it must not exist.

---

## 15. Definition of done

Dockerization is complete when:
- docker compose up starts all services
- Airflow UI is accessible
- Neo4j Browser is accessible
- DAGs run end-to-end
- Event graph persists across restarts

---

Status: Normative document.
