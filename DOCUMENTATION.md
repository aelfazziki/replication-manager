
### 3. Usage Documentation (`DOCUMENTATION.md`)

```markdown
# Replication Manager Documentation

## Key Features
- Drag-and-drop pipeline builder
- Oracle to BigQuery replication
- Schema conversion
- CDC with LogMiner/Binlog
- Web-based monitoring

## Quick Start Guide

1. **Add Endpoints**
   ```bash
   # Oracle
   curl -X POST -H "Content-Type: application/json" -d '{
     "name": "Prod Oracle",
     "type": "oracle",
     "config": {"host": "oracle-db", "port": 1521, "service": "ORCL"}
   }' http://localhost:5000/api/endpoints

   # BigQuery
   curl -X POST -H "Content-Type: application/json" -d '{
     "name": "Analytics DW",
     "type": "bigquery",
     "config": {"dataset": "analytics", "credentials": "service-account.json"}
   }' http://localhost:5000/api/endpoints