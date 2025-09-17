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

### Load
- Use Parquet/ORC/Avro for analytics, CSV/JSON only for interoperability.
- Always define write mode explicitly (overwrite, append, etc.).
- For partitioned data lakes, pick natural partitions (date/hour).
- For JDBC, avoid single-thread writes → partition + batch inserts.
- For streaming, checkpoint frequently for recovery.

### UDFs (User Defined Functions)
Sometimes built-in Spark functions (lower, trim, date_add, etc.) aren’t enough. That’s where UDFs come in: custom logic you can plug into DataFrame transformations.
- Use UDFs only as a last resort when Spark doesn’t have a built-in function.
- Keep UDF logic simple and deterministic.
- Always handle null inputs safely.
- Reuse registered UDFs across SQL/DataFrame queries.
#### Performance Considerations
- UDFs break Spark’s optimization (Catalyst can’t optimize them).
- Prefer built-in functions (regexp_replace, split, substring, when) when possible.
- For heavy usage, consider Spark SQL functions in Scala (functions API) instead of UDFs.
- If you need high performance, use Spark SQL native functions or pandas UDFs (PySpark side). In Scala, explore Dataset map/flatMap for efficiency.

### Joins
- Always filter early before joining → reduces shuffle size.
- Use broadcast joins when one dataset is < ~500 MB.
- When possible, repartition both DataFrames on the join key:
> val df1Part = df1.repartition($"id")
> val df2Part = df2.repartition($"id")
- Be explicit with column selection to avoid ambiguous schemas.
- Monitor join performance in Spark UI (look for shuffles).
