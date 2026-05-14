import os

from pyspark.sql import SparkSession


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# PostgreSQL Warehouse Configuration
pg_host = env("POSTGRES_DW_HOST", "postgres-dw")
pg_port = env("POSTGRES_DW_PORT", "5432")
pg_database = env("POSTGRES_DW_DB", "star_schema_dw")
pg_user = env("POSTGRES_DW_USER", "warehouse_user")
pg_password = env("POSTGRES_DW_PASSWORD", "warehouse_password")
pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

spark = (
    SparkSession.builder.appName("query-classicmodels-star-schema")
    .getOrCreate()
)


def jdbc_read_postgres(query: str) -> str:
    """Read from PostgreSQL using JDBC"""
    properties = {
        "url": pg_url,
        "driver": "org.postgresql.Driver",
        "user": pg_user,
        "password": pg_password,
    }
    return spark.read.format("jdbc").option("query", query).options(**properties).load()


print("Revenue by order month, customer country, and product line")
result = jdbc_read_postgres("""
    SELECT
      d.calendar_year,
      d.calendar_month,
      c.country,
      p.product_line,
      sum(f.quantity_ordered) AS units_sold,
      CAST(sum(f.gross_sales_amount) AS DECIMAL(12, 2)) AS gross_revenue,
      CAST(sum(f.margin_amount) AS DECIMAL(12, 2)) AS margin
    FROM star_schema.fact_order_sales f
    JOIN star_schema.dim_date d
      ON f.order_date_key = d.date_key
    JOIN star_schema.dim_customer c
      ON f.customer_key = c.customer_key
    JOIN star_schema.dim_product p
      ON f.product_key = p.product_key
    GROUP BY d.calendar_year, d.calendar_month, c.country, p.product_line
    ORDER BY d.calendar_year, d.calendar_month, c.country, p.product_line
""")

result.show(truncate=False)

print("Top customers by gross revenue")
result2 = jdbc_read_postgres("""
    SELECT
      c.customer_name,
      c.country,
      CAST(sum(f.gross_sales_amount) AS DECIMAL(12, 2)) AS gross_revenue
    FROM star_schema.fact_order_sales f
    JOIN star_schema.dim_customer c
      ON f.customer_key = c.customer_key
    GROUP BY c.customer_name, c.country
    ORDER BY gross_revenue DESC
""")

result2.show(truncate=False)

spark.stop()
