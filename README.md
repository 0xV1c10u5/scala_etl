# Scala ETL
Scala is exceptionally good at distributed ETL.  Aside from this use case, currently cannot think of using it regularly.  Therefore, I figured a collection of Scala snippets that are designed for ETL as they are needed would be helpful for code reuse in the future, and might help speed up development of efficient ETL pipelines in the future.

## Tips

### Extract
- Prefer Parquet/Avro/ORC over CSV/JSON for efficiency.
- Use explicit schemas when possible.
- For databases: push filters (.option("query", "...")) to reduce load.
- For S3/Cloud storage: partition your data by date/hour for faster reads.
- For streaming: handle schema evolution and late-arriving data.

### Transform
- Keep transformations stateless where possible (scales better).
- Use window functions for ranking, deduplication, or time-series.
- Minimize shuffles (joins, groupBy) by partitioning data wisely.
- Chain transformations fluently but cache intermediate results if reused.
- For readability, break complex pipelines into smaller vals.

### Cleaning
- Drop rows only when absolutely necessary (consider downstream impact).
- Standardize strings early to avoid join mismatches.
- Track cleaning decisions (audit trail, logging).
- For large datasets: prefer fill over drop (retains more data).
- Outlier handling should be domain-driven (don’t just clip blindly).
