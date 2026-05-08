# Classicmodels MySQL to Iceberg Star Schema

Project này mock một pipeline Lakehouse đúng theo assignment:

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

## Chạy Đồng Bộ Định Kỳ

Chạy service sync:

```bash
docker compose --profile scheduler up sync
```

Mặc định mỗi 300 giây service này chạy lại Spark ETL một lần. Có thể đổi interval:

```bash
SYNC_INTERVAL_SECONDS=60 docker compose --profile scheduler up sync
```

## Test Source Thay Đổi

Sau khi đã chạy ETL lần đầu, áp dụng script thay đổi dữ liệu nguồn:

```bash
docker exec -i classicmodels-mysql mysql -uroot -proot classicmodels < mysql/examples/01_make_source_changes.sql
```

Sau đó chạy lại ETL một lượt:

```bash
docker compose run --rm etl
```

Hoặc để service `sync` tự bắt thay đổi ở lần chạy định kỳ tiếp theo.

Query lại star schema:

```bash
docker compose run --rm query
```

## Các Bảng Nguồn Classicmodels

MySQL source có các bảng classicmodels chính:

- `offices`
- `employees`
- `customers`
- `productlines`
- `products`
- `orders`
- `orderdetails`
- `payments`

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

## Cách Đồng Bộ Hoạt Động

Mỗi lượt ETL:

1. Đọc các bảng MySQL classicmodels qua JDBC.
2. Tạo staging dataframe cho từng bảng.
3. Tính `_row_hash` từ toàn bộ cột source.
4. `MERGE INTO local.bronze.<table>` theo primary key.
5. Dòng mới được insert.
6. Dòng đổi dữ liệu được update.
7. Dòng không còn trong MySQL được đánh dấu `_is_deleted = true`.
8. Rebuild các bảng star schema từ bronze active rows.

Đây là cách phù hợp cho assignment và dataset nhỏ. Trong production lớn hơn, nên dùng `updated_at` hoặc CDC như Debezium thay vì đọc full snapshot mỗi kỳ.
