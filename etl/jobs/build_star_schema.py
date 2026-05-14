import os
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp


SOURCE_TABLES: Dict[str, List[str]] = {
    "offices": ["officeCode"],
    "employees": ["employeeNumber"],
    "customers": ["customerNumber"],
    "productlines": ["productLine"],
    "products": ["productCode"],
    "orders": ["orderNumber"],
    "orderdetails": ["orderNumber", "productCode"],
    "payments": ["customerNumber", "checkNumber"],
}


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# MySQL Source Configuration
mysql_host = env("MYSQL_HOST", "mysql")
mysql_port = env("MYSQL_PORT", "3306")
mysql_database = env("MYSQL_DATABASE", "classicmodels")
mysql_user = env("MYSQL_USER", "etl")
mysql_password = env("MYSQL_PASSWORD", "etl")

# PostgreSQL Warehouse Configuration
pg_host = env("POSTGRES_DW_HOST", "postgres-dw")
pg_port = env("POSTGRES_DW_PORT", "5432")
pg_database = env("POSTGRES_DW_DB", "star_schema_dw")
pg_user = env("POSTGRES_DW_USER", "warehouse_user")
pg_password = env("POSTGRES_DW_PASSWORD", "warehouse_password")
pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

spark = (
    SparkSession.builder.appName("classicmodels-mysql-to-postgres-star-schema")
    .getOrCreate()
)


def jdbc_read_mysql(table: str) -> DataFrame:
    """Read table from MySQL using JDBC"""
    url = (
        f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table)
        .option("user", mysql_user)
        .option("password", mysql_password)
        .load()
    )


def jdbc_write_postgres(df: DataFrame, schema: str, table: str, mode: str = "overwrite") -> None:
    """Write DataFrame to PostgreSQL using JDBC"""
    table_name = f"{schema}.{table}"
    properties = {
        "url": pg_url,
        "driver": "org.postgresql.Driver",
        "dbtable": table_name,
        "user": pg_user,
        "password": pg_password,
    }
    df.write.format("jdbc").mode(mode).options(**properties).save()



def key_expr(column: str) -> str:
    """Generate a hash-based key expression"""
    return f"abs(hash({column}))"


def nullable_key_expr(column: str) -> str:
    """Generate a nullable hash-based key expression"""
    return f"CASE WHEN {column} IS NULL THEN NULL ELSE {key_expr(column)} END"


# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS star_schema")

# Read all source tables from MySQL
print("Reading classicmodels source tables from MySQL...")
source_data = {}
for source_table in SOURCE_TABLES.keys():
    source_data[source_table] = jdbc_read_mysql(source_table)
    print(f"  loaded {source_table}: {source_data[source_table].count()} rows")

# Create temporary views for easier querying
for table_name, df in source_data.items():
    df.createOrReplaceTempView(table_name)

# Build dimensions
print("Building star schema dimensions...")

# Dimension: dim_customer
dim_customer = spark.sql("""
    SELECT
      abs(hash(c.customerNumber)) AS customer_key,
      c.customerNumber,
      c.customerName,
      c.contactFirstName,
      c.contactLastName,
      c.phone,
      c.addressLine1,
      c.addressLine2,
      c.city,
      c.state,
      c.postalCode,
      c.country,
      CAST(CASE WHEN c.salesRepEmployeeNumber IS NULL THEN NULL ELSE abs(hash(c.salesRepEmployeeNumber)) END AS BIGINT) AS sales_rep_employee_key,
      c.salesRepEmployeeNumber,
      CAST(c.creditLimit AS DECIMAL(12, 2)) AS creditLimit,
      current_timestamp() AS etl_loaded_at
    FROM customers c
""")
jdbc_write_postgres(dim_customer, "star_schema", "dim_customer", "overwrite")
print(f"  created dim_customer: {dim_customer.count()} rows")

# Dimension: dim_product
dim_product = spark.sql("""
    SELECT
      abs(hash(p.productCode)) AS product_key,
      p.productCode,
      p.productName,
      p.productLine,
      pl.textDescription AS productLineDescription,
      p.productScale,
      p.productVendor,
      p.productDescription,
      CAST(p.quantityInStock AS INT) AS quantityInStock,
      CAST(p.buyPrice AS DECIMAL(10, 2)) AS buyPrice,
      CAST(p.MSRP AS DECIMAL(10, 2)) AS msrp,
      current_timestamp() AS etl_loaded_at
    FROM products p
    LEFT JOIN productlines pl
      ON p.productLine = pl.productLine
""")
jdbc_write_postgres(dim_product, "star_schema", "dim_product", "overwrite")
print(f"  created dim_product: {dim_product.count()} rows")

# Dimension: dim_employee
dim_employee = spark.sql("""
    SELECT
      abs(hash(e.employeeNumber)) AS employee_key,
      e.employeeNumber,
      e.firstName,
      e.lastName,
      concat(e.firstName, ' ', e.lastName) AS fullName,
      e.extension,
      e.email,
      e.jobTitle,
      CAST(CASE WHEN e.reportsTo IS NULL THEN NULL ELSE abs(hash(e.reportsTo)) END AS BIGINT) AS manager_employee_key,
      e.reportsTo,
      abs(hash(e.officeCode)) AS office_key,
      e.officeCode,
      current_timestamp() AS etl_loaded_at
    FROM employees e
""")
jdbc_write_postgres(dim_employee, "star_schema", "dim_employee", "overwrite")
print(f"  created dim_employee: {dim_employee.count()} rows")

# Dimension: dim_office
dim_office = spark.sql("""
    SELECT
      abs(hash(o.officeCode)) AS office_key,
      o.officeCode,
      o.city,
      o.phone,
      o.addressLine1,
      o.addressLine2,
      o.state,
      o.country,
      o.postalCode,
      o.territory,
      current_timestamp() AS etl_loaded_at
    FROM offices o
""")
jdbc_write_postgres(dim_office, "star_schema", "dim_office", "overwrite")
print(f"  created dim_office: {dim_office.count()} rows")

# Dimension: dim_date
dim_date = spark.sql("""
    WITH all_dates AS (
      SELECT orderDate AS calendar_date FROM orders
      UNION ALL
      SELECT requiredDate AS calendar_date FROM orders
      UNION ALL
      SELECT shippedDate AS calendar_date FROM orders WHERE shippedDate IS NOT NULL
      UNION ALL
      SELECT paymentDate AS calendar_date FROM payments
    ),
    bounds AS (
      SELECT min(calendar_date) AS min_date, max(calendar_date) AS max_date
      FROM all_dates
    ),
    dates AS (
      SELECT explode(sequence(min_date, max_date, interval 1 day)) AS calendar_date
      FROM bounds
    )
    SELECT
      CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
      calendar_date,
      year(calendar_date) AS calendar_year,
      quarter(calendar_date) AS calendar_quarter,
      month(calendar_date) AS calendar_month,
      date_format(calendar_date, 'MMMM') AS month_name,
      dayofmonth(calendar_date) AS day_of_month,
      dayofweek(calendar_date) AS day_of_week,
      date_format(calendar_date, 'EEEE') AS day_name,
      CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend
    FROM dates
""")
jdbc_write_postgres(dim_date, "star_schema", "dim_date", "overwrite")
print(f"  created dim_date: {dim_date.count()} rows")

# Build facts
print("Building star schema facts...")

# Fact: fact_order_sales
fact_order_sales = spark.sql("""
    SELECT
      concat(CAST(od.orderNumber AS STRING), '-', od.productCode) AS sales_line_id,
      od.orderNumber,
      od.orderLineNumber,
      abs(hash(o.customerNumber)) AS customer_key,
      abs(hash(od.productCode)) AS product_key,
      CAST(CASE WHEN c.salesRepEmployeeNumber IS NULL THEN NULL ELSE abs(hash(c.salesRepEmployeeNumber)) END AS BIGINT) AS sales_rep_employee_key,
      CAST(CASE WHEN e.officeCode IS NULL THEN NULL ELSE abs(hash(e.officeCode)) END AS BIGINT) AS sales_office_key,
      CAST(date_format(o.orderDate, 'yyyyMMdd') AS INT) AS order_date_key,
      CAST(date_format(o.requiredDate, 'yyyyMMdd') AS INT) AS required_date_key,
      CASE
        WHEN o.shippedDate IS NULL THEN NULL
        ELSE CAST(date_format(o.shippedDate, 'yyyyMMdd') AS INT)
      END AS shipped_date_key,
      o.orderDate,
      o.requiredDate,
      o.shippedDate,
      o.status AS order_status,
      CAST(od.quantityOrdered AS INT) AS quantity_ordered,
      CAST(od.priceEach AS DECIMAL(10, 2)) AS price_each,
      CAST(p.buyPrice AS DECIMAL(10, 2)) AS buy_price,
      CAST(p.MSRP AS DECIMAL(10, 2)) AS msrp,
      CAST(od.quantityOrdered * od.priceEach AS DECIMAL(12, 2)) AS gross_sales_amount,
      CAST(od.quantityOrdered * p.buyPrice AS DECIMAL(12, 2)) AS cost_amount,
      CAST(od.quantityOrdered * (od.priceEach - p.buyPrice) AS DECIMAL(12, 2)) AS margin_amount,
      current_timestamp() AS etl_loaded_at
    FROM orderdetails od
    JOIN orders o
      ON od.orderNumber = o.orderNumber
    JOIN customers c
      ON o.customerNumber = c.customerNumber
    JOIN products p
      ON od.productCode = p.productCode
    LEFT JOIN employees e
      ON c.salesRepEmployeeNumber = e.employeeNumber
    WHERE o.status <> 'Cancelled'
""")
jdbc_write_postgres(fact_order_sales, "star_schema", "fact_order_sales", "overwrite")
print(f"  created fact_order_sales: {fact_order_sales.count()} rows")

# Fact: fact_payments
fact_payments = spark.sql("""
    SELECT
      concat(CAST(p.customerNumber AS STRING), '-', p.checkNumber) AS payment_id,
      p.customerNumber,
      p.checkNumber,
      abs(hash(p.customerNumber)) AS customer_key,
      CAST(CASE WHEN c.salesRepEmployeeNumber IS NULL THEN NULL ELSE abs(hash(c.salesRepEmployeeNumber)) END AS BIGINT) AS sales_rep_employee_key,
      CAST(date_format(p.paymentDate, 'yyyyMMdd') AS INT) AS payment_date_key,
      p.paymentDate,
      CAST(p.amount AS DECIMAL(12, 2)) AS payment_amount,
      current_timestamp() AS etl_loaded_at
    FROM payments p
    JOIN customers c
      ON p.customerNumber = c.customerNumber
""")
jdbc_write_postgres(fact_payments, "star_schema", "fact_payments", "overwrite")
print(f"  created fact_payments: {fact_payments.count()} rows")

print("Pipeline complete. Star schema tables built in PostgreSQL:")
for table in [
    "dim_customer",
    "dim_product",
    "dim_employee",
    "dim_office",
    "dim_date",
    "fact_order_sales",
    "fact_payments",
]:
    try:
        count = spark.sql(f"SELECT COUNT(*) FROM star_schema.{table}").collect()[0][0]
        print(f"  star_schema.{table}: {count} rows")
    except Exception as e:
        print(f"  star_schema.{table}: error - {e}")

spark.stop()

