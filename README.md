# 🐋 Crypto Whale Watch
_Real-time on-chain analytics dashboard tracking large cryptocurrency movements_

## Architecture
```
[Etherscan API] → [Python Ingestion] → [DuckDB] → [dbt Transforms] → [Streamlit Dashboard]
```

## Stack
- **Ingestion:** Python + requests
- **Storage:** DuckDB
- **Transformation:** dbt-core + dbt-duckdb
- **Orchestration:** Dagster (TBD)
- **Dashboard:** Streamlit + Plotly
- **CI/CD:** GitHub Actions

## Setup
```bash
pip install -r requirements.txt
cp .env.example .env
# Add your Etherscan API key to .env
python ingest/ingest.py
```

## Status
- [x] Project scaffold
- [x] Milestone 1: First data pull
- [x] Milestone 2: dbt models
- [x] Milestone 3: Orchestration
- [x] Milestone 4: Dashboard
- [ ] Milestone 5: Ship it
