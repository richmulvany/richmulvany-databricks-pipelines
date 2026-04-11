# Runbook: Add a New Gold Table

## Steps

### 1. Define the Gold table

Create `pipeline/gold/your_table_name.py`:

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold_your_table_name",
    comment="Description of what this table contains and who uses it.",
    table_properties={"quality": "gold"},
)
def gold_your_table_name():
    return (
        dlt.read("silver_entities")
        # Your aggregation/transformation logic here
        .groupBy("category")
        .agg(F.count("id").alias("count"))
    )
```

### 2. Register it in the pipeline

Add the new notebook path to `databricks.yml` under `resources.pipelines.medallion_pipeline.libraries`.

### 3. Add data quality expectations

Add any relevant expectations directly on the `@dlt.table` decorator using `@dlt.expect_or_fail`.

### 4. Add to the export script

In `scripts/export_gold_tables.py`, add an entry to `GOLD_TABLE_EXPORTS`:

```python
GOLD_TABLE_EXPORTS = {
    "entity_summary": ...,
    "your_table_name": f"SELECT * FROM {CATALOG}.{SCHEMA}.gold_your_table_name",
}
```

### 5. Update the data dictionary

Add an entry to `docs/data_dictionary/README.md`.

### 6. Add a frontend API function

Add a fetch function in `frontend/src/api/` and a corresponding hook in `frontend/src/hooks/`.

### 7. Deploy

```bash
make deploy-pipeline
```
