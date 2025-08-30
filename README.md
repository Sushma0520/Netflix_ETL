# Netflix ETL Pipeline (Apache NiFi ➜ PySpark ➜ PostgreSQL)

## Overview

This project builds a small but realistic ETL pipeline for Netflix data. We preprocess CSVs in Apache NiFi (including a header rename), then clean and load with PySpark into PostgreSQL for analytics.

**Key goals**

* Make ingestion reproducible and idempotent
* Normalize column names and types
* Land data in a warehouse-friendly schema for SQL analysis

---

## High-Level Architecture

```
CSV files (raw)
   └── NiFi (GetFile → ReplaceText)  # header fix: cast → cast_member
         └── (failed files)PutFile to a "staging" folder
              └── PutDatabaseRecord → postgres.netflix_raw
PySpark (read postgres.netflix_raw → clean → write postgres.netflix_clean)
PostgreSQL (SQL analytics)
```

---

## What’s special in this repo

* **Header rename done in NiFi**: `cast` → `cast_member` **at the CSV header only**, *not* values.
* **PySpark 3.5.x + Java 11** for compatibility and stability on Windows.
* **NiFi locked to Java 17** via `bin/nifi-env.bat` while the system `JAVA_HOME` stays on Java 11 for Spark.
* `netflix_clean` is written to **`public.netflix_clean`** (PostgreSQL default schema).

---

## 1) NiFi Pre‑Processing (Windows)

### Java setup for NiFi

In `nifi-<version>\bin\nifi-env.bat` add at the top:

```bat
set JAVA_HOME=C:\Program Files\Java\jdk-17
```

This forces NiFi to use Java 17 regardless of the system `JAVA_HOME`.

### Minimal Flow (used here)

```
GetFile → ReplaceText → (optional) PutFile
```

* **GetFile**

  * *Input Directory*: folder containing the raw Netflix CSV(s)
  * *Keep Source File*: true/false (as you prefer)

* **ReplaceText** (rename only the header column `cast` → `cast_member`)

 * *Relacement Strategy*: Regex Replace
 * *Search Value*: (?<=,)\s*cast\s*(?=,)
 * *Replacement Value*: cast_member
 * *Evaluation Mode*: Line-by-Line
 * *Line-by-Line Evaluation Mode*: All
 * *Note*: Using commas,?,<,= ensures only the **header token** changes and avoids touching values that might contain the word "cast".

* **(Optional) PutFile**

  * Writes the header-fixed CSV to a staging folder for auditing or later loads in case any file failed and it can also be used after GetFile.

### (Optional) If you later use Record Readers/Writers

If you go back to record-aware processors (e.g., `UpdateRecord`, `PutDatabaseRecord`):

* **CSVReader**

  * *CSV Format*: `Custom`
  * *Value Separator*: `,`
  * *Record Separator*: `\n` (or `\r\n` on Windows files)
  * *Quote Character*: `"`
  * *Treat First Line as Header*: `true`
  * *Allow Unquoted Fields*: `true`
  * *Trim Fields*: `true`
* **CSVRecordSetWriter**

  * *Include Header Line*: `true` (so the corrected header is written)

### (Optional) PutDatabaseRecord → PostgreSQL (raw landing)

If you choose to land raw data straight into Postgres from NiFi:

* Use a `DBCPConnectionPool` controller service (Postgres JDBC URL/user/pass).
* Configure **`PutDatabaseRecord`**

  * *Table Name*: `netflix_raw`
  * *Unmatched Column Behavior*: `Ignore`
  * *Translate Field Names*: `true`
  * Ensure your CSVReader detects the corrected header names (e.g., `cast_member`).

> In this project we proceeded with **PySpark** reading from an existing `netflix_raw` table, but the NiFi path above remains a valid alternative.

---

## 2) PySpark Transform + Load to PostgreSQL

### Environment

* **Python**: 3.10
* **Java**: 11 (system `JAVA_HOME`)
* **PySpark**: 3.5.x (e.g., 3.5.6)
* **PostgreSQL JDBC**: 42.7.6 (added via `spark.jars`)

> If you ever move to **PySpark 4.x**, switch to **Java 17**.

### ETL Script (key parts)

* Reads `public.netflix_raw`
* Casts `release_year` → INT
* Parses `date_added` → DATE (format: `MMMM d, yyyy`)
* Writes to `public.netflix_clean`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType

#  Start Spark Session with JDBC jar
spark = SparkSession.builder \
    .appName("NetflixDataCleaning") \
    .config("spark.jars", "file:///C:/Users/giris/Downloads/nifi-1.25.0-bin/nifi-1.25.0/lib/postgresql-42.7.6.jar") \
    .getOrCreate()

print(" Spark session started")

#  Read raw Netflix data from PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/Netflix_db"
jdbc_properties = {
    "user": "postgres",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

try:
    raw_df = spark.read.jdbc(url=jdbc_url, table="netflix_raw", properties=jdbc_properties)
    print(" Raw DataFrame Schema:")
    raw_df.printSchema()
except Exception as e:
    print(" Error reading raw table:", e)
    spark.stop()
    exit(1)

#  Clean data
clean_df = raw_df \
    .withColumn("release_year", col("release_year").cast(IntegerType())) \
    .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

print(" Cleaned DataFrame Schema:")
clean_df.printSchema()

#  Write cleaned data back to PostgresSQL
#     No need for createTableColumnTypes — let Spark handle it
try:
    clean_df.write.jdbc(
        url=jdbc_url,
        table="netflix_clean",
        mode="overwrite",   # Overwrites if table already exists
        properties=jdbc_properties
    )
    print(" Cleaned data written to table: netflix_clean")
except Exception as e:
    print(" Error writing cleaned table:", e)

spark.stop()
```

> We allow Spark to infer column types on write. If you need strict DDL, pass `createTableColumnTypes`.

---

## 3) Verifying in PostgreSQL

```sql
-- See tables
\dt

-- Peek data
SELECT * FROM public.netflix_clean LIMIT 5;

-- Top 10 countries by volume
SELECT country, COUNT(*) AS total_titles
FROM public.netflix_clean
GROUP BY country
ORDER BY total_titles DESC
LIMIT 10;
```

> Why `public.netflix_clean`? Because Postgres defaults to the `public` schema when no schema is specified.

---

## Roadmap

* Automate raw landing with `PutDatabaseRecord` to `etl.netflix_raw`
* Add data quality checks (row counts, null checks) in PySpark
* Build a small BI dashboard

---

## Author

**Sushma**
Aspiring Data Engineer | SQL DBA → Data Engineering
GitHub: `Sushma0520`
