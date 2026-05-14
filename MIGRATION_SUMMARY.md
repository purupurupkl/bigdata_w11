# Migration from MinIO/Iceberg to PostgreSQL Lakehouse

## Summary
Successfully migrated the data lakehouse architecture from MinIO (S3-compatible object storage) with Iceberg tables to PostgreSQL as a direct data warehouse solution.

## Changes Made

### 1. **docker-compose.yml**
- **Removed:**
  - MinIO service configuration
  - S3A/Iceberg Spark configurations
  - `create-bucket` service dependencies
  - `minio_data` volume

- **Updated:**
  - `x-spark-common` environment variables:
    - Removed: `ICEBERG_CATALOG`, `ICEBERG_WAREHOUSE`, `ICEBERG_BRONZE_NAMESPACE`, `ICEBERG_STAR_NAMESPACE`
    - Added: `POSTGRES_DW_HOST`, `POSTGRES_DW_PORT`, `POSTGRES_DW_DB`, `POSTGRES_DW_USER`, `POSTGRES_DW_PASSWORD`
  - Spark packages: Removed Iceberg, Hadoop-AWS; kept MySQL connector and PostgreSQL driver
  - Spark submit config: Removed all S3A/MinIO configurations
  
- **Services updated:**
  - `sync`, `query`, `airflow-webserver`, `airflow-scheduler` no longer depend on `create-bucket`
  - All depend on `postgres-dw` service instead
  
- **Volumes:**
  - Added `dw_data` volume for PostgreSQL persistence

### 2. **etl/jobs/build_star_schema.py**
- **Removed:**
  - Iceberg catalog configuration
  - Bronze layer (no intermediate Iceberg tables)
  - MERGE INTO operations on Iceberg tables
  - Iceberg-specific functions (`create_namespace`, `table_exists`, `sync_bronze_table`, `replace_table_from_query`)

- **Added:**
  - PostgreSQL JDBC configuration and connection
  - Direct MySQL to PostgreSQL JDBC write operations
  - Simplified architecture: MySQL source → Spark transforms → PostgreSQL star schema

- **Architecture:**
  - **Old:** MySQL → Spark → Iceberg Bronze (MinIO) → Spark → Iceberg Star Schema (MinIO)
  - **New:** MySQL → Spark → PostgreSQL Star Schema

- **Star schema tables (all now in PostgreSQL `star_schema` schema):**
  - `dim_customer`
  - `dim_product`
  - `dim_employee`
  - `dim_office`
  - `dim_date`
  - `fact_order_sales`
  - `fact_payments`

### 3. **etl/jobs/query_star_schema.py**
- **Removed:**
  - Iceberg catalog configuration
  - Spark SQL queries on Iceberg tables
  - SHOW TABLES operation on Iceberg

- **Added:**
  - PostgreSQL JDBC configuration
  - JDBC-based SQL queries to PostgreSQL star schema

- **Queries:**
  - Revenue by order month, customer country, and product line
  - Top customers by gross revenue

### 4. **airflow/dags/classicmodels_lakehouse_dag.py**
- **Updated packages:** 
  - Removed Iceberg and Hadoop-AWS packages
  - Kept MySQL and PostgreSQL drivers

- **Updated Spark configuration:**
  - Removed all Iceberg and S3A-related configs
  - Simplified to basic Spark setup

- **Updated environment variables:**
  - Added PostgreSQL connection details
  - Removed Iceberg catalog references

### 5. **Dockerfile**
- **Removed:**
  - MinIO client installation

- **Added:**
  - PostgreSQL client (`postgresql-client`) for database health checks

## Architecture Comparison

### Before (MinIO + Iceberg)
```
MySQL (Source)
  ↓
Spark ETL
  ↓
Iceberg Bronze Tables (MinIO s3a://warehouse/iceberg/bronze)
  ↓
Spark Transformations
  ↓
Iceberg Star Schema (MinIO s3a://warehouse/iceberg/star_schema)
```

### After (PostgreSQL)
```
MySQL (Source)
  ↓
Spark ETL
  ↓
PostgreSQL Star Schema (star_schema_dw.star_schema)
```

## Benefits

1. **Simplified Infrastructure:** No need for MinIO and object storage
2. **Reduced Complexity:** No Iceberg catalog management
3. **Direct Database Query:** Query star schema directly from PostgreSQL
4. **Lower Resource Usage:** Eliminate object storage layer
5. **Easier Debugging:** Direct SQL access to all tables
6. **Better Integration:** Native PostgreSQL integration with most BI tools

## Configuration

### PostgreSQL Connection Details
- **Host:** `postgres-dw`
- **Port:** `5432`
- **Database:** `star_schema_dw`
- **User:** `warehouse_user`
- **Password:** `warehouse_password`
- **Schema:** `star_schema`

### Running the Pipeline

**Using docker-compose:**
```bash
docker-compose up -d mysql postgres-dw
docker-compose up etl
docker-compose up query --profile tools
```

**Using Airflow:**
```bash
docker-compose --profile airflow up
```

## Database Connection

To connect to the PostgreSQL data warehouse:
```bash
psql -h postgres-dw -U warehouse_user -d star_schema_dw
```

## Verification

After running the pipeline, verify the tables:
```sql
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'star_schema' 
ORDER BY table_name;
```

Query example:
```sql
SELECT 
  calendar_year,
  calendar_month,
  country,
  product_line,
  SUM(quantity_ordered) AS units_sold,
  SUM(gross_sales_amount) AS gross_revenue
FROM star_schema.fact_order_sales f
JOIN star_schema.dim_date d ON f.order_date_key = d.date_key
JOIN star_schema.dim_customer c ON f.customer_key = c.customer_key
JOIN star_schema.dim_product p ON f.product_key = p.product_key
GROUP BY calendar_year, calendar_month, country, product_line
ORDER BY calendar_year DESC, calendar_month DESC;
```

## Files Modified

1. `/docker-compose.yml` - Removed MinIO, updated Spark config
2. `/etl/jobs/build_star_schema.py` - Rewrote for PostgreSQL direct writes
3. `/etl/jobs/query_star_schema.py` - Updated to query PostgreSQL
4. `/airflow/dags/classicmodels_lakehouse_dag.py` - Updated DAG configuration
5. `/Dockerfile` - Replaced MinIO client with PostgreSQL client

## Notes

- All Spark packages now use standard JDBC connectors (MySQL and PostgreSQL)
- No external object storage needed
- Data persists in PostgreSQL volumes
- Hash-based surrogate keys are still generated for dimensions
- Fact tables are created fresh on each ETL run (overwrite mode)
