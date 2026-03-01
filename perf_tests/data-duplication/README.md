# Data Duplication Scripts (Neo4j & ArangoDB)

Scripts to duplicate connector data (RecordGroups, Records, and related edges) for a given connector. Use this to generate larger datasets for load and latency testing.

| Script | Database |
|--------|----------|
| `duplicate_connector_data.py` | Neo4j |
| `duplicate_connector_data_arango.py` | ArangoDB |

These scripts depend on this repo's `app` package (Neo4j/Arango clients and config). To use them from another repo, copy the script file(s) into this repo (e.g. under `backend/python/scripts/`) and run from here.

---

## Setup

1. Use the Python environment for this backend (create/activate venv from `backend/python`):

   ```bash
   cd backend/python
   python -m venv venv
   source venv/Scripts/activate   # Windows
   # source venv/bin/activate     # Linux/macOS
   pip install -r requirements.txt
   ```

2. Set environment variables (e.g. from `backend/python/.env` or export in the shell):

   **Neo4j**

   - `NEO4J_URI` (default: `bolt://localhost:7687`)
   - `NEO4J_USERNAME` (default: `neo4j`)
   - `NEO4J_PASSWORD` (required)

   **ArangoDB**

   - `ARANGO_URL` (default: `http://localhost:8529`)
   - `ARANGO_USERNAME` (default: `root`)
   - `ARANGO_PASSWORD` (required)
   - `ARANGO_DB_NAME` (database name, e.g. `es`)

   Example (Linux/macOS): `export $(grep -v '^#' .env | xargs)`  
   (Run from `backend/python` so `.env` is in the current directory.)

---

## Usage

Run from **`backend/python`** so that `app` and `scripts` resolve correctly.

### Neo4j

```bash
cd backend/python

# Preview (dry run)
python -m scripts.duplicate_connector_data --connector-id <CONNECTOR_ID> --copies <N> --dry-run

# Create copies
python -m scripts.duplicate_connector_data --connector-id <CONNECTOR_ID> --copies <N>

# Optional: batch size (default 500)
python -m scripts.duplicate_connector_data --connector-id <CONNECTOR_ID> --copies <N> --batch-size 1000
```

### ArangoDB

```bash
cd backend/python

# Preview (dry run)
python -m scripts.duplicate_connector_data_arango --connector-id <CONNECTOR_ID> --copies <N> --dry-run

# Create copies
python -m scripts.duplicate_connector_data_arango --connector-id <CONNECTOR_ID> --copies <N>

# Optional: batch size (default 500)
python -m scripts.duplicate_connector_data_arango --connector-id <CONNECTOR_ID> --copies <N> --batch-size 1000
```

---

## Arguments

| Argument         | Required | Description                                        |
|------------------|----------|----------------------------------------------------|
| `--connector-id` | Yes      | Connector ID to duplicate data for                |
| `--copies`       | Yes      | Number of duplicate copies (1–100)                 |
| `--dry-run`      | No       | Only fetch and report counts; do not write data    |
| `--batch-size`   | No       | Batch size for DB operations (default: 500)        |

Duplicated records can be identified by the `_copyN` suffix on external IDs (e.g. `externalGroupId`, `externalRecordId`).
