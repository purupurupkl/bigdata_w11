# Classicmodels MySQL to Iceberg Star Schema


```text
MySQL classicmodels
  -> Spark ETL định kỳ
  -> Iceberg bronze tables trên MinIO
  -> Iceberg star schema tables trên MinIO
```

Stack gồm: 

- `mysql`: source RDBMS, seed database `classicmodels`.
- `minio`: object storage để lưu Iceberg warehouse.
- `create-bucket`: tạo bucket `warehouse` trong MinIO.
- `etl`: chạy một lượt ETL.
- `sync`: chạy ETL định kỳ, mặc định mỗi 300 giây.
- `query`: chạy vài câu query kiểm tra star schema.

## Chạy Lần Đầu

Nếu trước đó đã chạy bản mock cũ, xóa volume cũ để MySQL seed lại schema `classicmodels`:

```bash
docker compose down -v
```

Chạy một lượt ETL:

```bash
docker compose up etl
```

Query kết quả:

```bash
docker compose run --rm query
```

Mở MinIO console:

```text
http://localhost:9001
user: minioadmin
password: minioadmin
```

Iceberg warehouse nằm trong bucket/path:

```text
s3a://warehouse/iceberg
```

## Đồng bộ:

Chạy service sync:

```bash
docker compose --profile scheduler up sync
```

## Airflow Orchestration

Bai tap moi yeu cau scheduling, orchestration va monitoring bang Airflow hoac Dagster. Project nay dung Airflow de dieu phoi pipeline Spark ETL da co.

Chay Airflow:

```bash
docker compose --profile airflow up airflow-webserver airflow-scheduler
```

Mo Airflow UI:

```text
http://localhost:8080
user: admin
password: admin
```

DAG:

```text
classicmodels_mysql_to_iceberg_star_schema
```

Workflow trong DAG:

```text
start
  -> check_mysql_ready
  -> check_minio_ready
  -> run_spark_etl
  -> validate_star_schema
  -> end
```

Y nghia cac task:

- `check_mysql_ready`: kiem tra MySQL source da san sang.
- `check_minio_ready`: kiem tra MinIO bucket `warehouse` da san sang.
- `run_spark_etl`: chay Spark ETL, doc MySQL qua JDBC va ghi Iceberg tren MinIO.
- `validate_star_schema`: chay Spark SQL query de kiem tra bang star schema doc duoc.

Scheduling:

- DAG duoc lap lich moi 1 phut bang `schedule=timedelta(minutes=1)`.
- `max_active_runs=1` de tranh nhieu lan ETL chay chong len nhau.

Orchestration:

- Airflow quan ly thu tu task.
- Neu check MySQL/MinIO fail thi ETL khong chay.
- Neu ETL fail thi validation query khong chay.

Monitoring:

- Airflow UI hien thi Graph, Grid, lich su lan chay, trang thai tung task va logs.
- Nen screenshot Graph view, Grid view, task logs cua `run_spark_etl`, va task logs cua `validate_star_schema`.

## Bronze Layer

Bronze là lớp Iceberg lưu dữ liệu gần giống source nhất. ETL đọc toàn bộ từng bảng MySQL, tính `_row_hash` cho mỗi dòng, rồi đồng bộ vào Iceberg theo primary key.

Bronze tables:

- `local.bronze.offices`
- `local.bronze.employees`
- `local.bronze.customers`
- `local.bronze.productlines`
- `local.bronze.products`
- `local.bronze.orders`
- `local.bronze.orderdetails`
- `local.bronze.payments`

Mỗi bảng bronze có thêm metadata:

- `_row_hash`: phát hiện update.
- `_is_deleted`: đánh dấu dòng đã bị xóa ở source.
- `_synced_at`: thời điểm sync gần nhất.

Vì classicmodels gốc không có `updated_at`, pipeline dùng cách snapshot compare: mỗi lượt định kỳ đọc source, so sánh hash theo primary key, rồi `MERGE INTO` Iceberg. Insert/update được upsert; delete được đánh dấu `_is_deleted = true`.

## Star Schema

Star schema được build từ các bronze rows còn active:

- `local.star_schema.dim_customer`
- `local.star_schema.dim_product`
- `local.star_schema.dim_employee`
- `local.star_schema.dim_office`
- `local.star_schema.dim_date`
- `local.star_schema.fact_order_sales`
- `local.star_schema.fact_payments`

`fact_order_sales` có grain là một dòng order detail:

```text
1 row = 1 product trong 1 order
```

Fact này join tới:

- customer dimension
- product dimension
- sales representative employee dimension
- office dimension
- date dimension

Các measure chính:

- `quantity_ordered`
- `price_each`
- `gross_sales_amount`
- `cost_amount`
- `margin_amount`

`fact_payments` có grain là một payment/check của customer.

# Pipeline ETL (Python + Spark)

- build_star_schema.py:
  Dùng Spark đọc dữ liệu từ MySQL classicmodels qua JDBC.
  Đồng bộ các bảng nguồn vào Iceberg bronze tables trên MinIO.
  Tạo surrogate keys cho dimension/fact.
  Chuyển dữ liệu từ mô hình OLTP sang Star Schema.
  Ghi các bảng dimension và fact vào Iceberg warehouse trên MinIO.

- query_star_schema.py:
  Dùng Spark SQL kết nối đến Iceberg catalog.
  Đọc các bảng Star Schema từ MinIO.
  Chạy các truy vấn kiểm tra như doanh thu theo tháng/quốc gia/product line
  và top customers theo gross revenue.

- sync service trong docker-compose.yml:
  Chạy lại build_star_schema.py theo chu kỳ.
  Mặc định mỗi 300 giây, có thể đổi thành 60 giây bằng SYNC_INTERVAL_SECONDS.
  Mỗi lần chạy, pipeline đọc lại source MySQL, so sánh với bronze bằng primary key
  và _row_hash, sau đó MERGE INTO Iceberg để insert/update/mark deleted rows.
