# Hệ thống Phát hiện Gian lận Thời gian thực

## Tổng quan

Dự án này triển khai một hệ thống phát hiện gian lận thời gian thực sử dụng kiến trúc xử lý dữ liệu luồng (streaming). Hệ thống giám sát các giao dịch tài chính, phát hiện các giao dịch đáng ngờ dựa trên các quy tắc và cảnh báo kịp thời.

## Kiến trúc hệ thống

![Kiến trúc Hệ thống](./docs/system_architecture.png)

Hệ thống bao gồm các thành phần chính sau:

1. **Kafka**: Message broker xử lý luồng dữ liệu giao dịch
2. **Spark Streaming**: Xử lý dữ liệu thời gian thực và phát hiện gian lận
3. **PostgreSQL**: Lưu trữ lịch sử giao dịch và kết quả phân tích
4. **Elasticsearch**: Đánh chỉ mục giao dịch để tìm kiếm và truy vấn nhanh
5. **Alert Service**: Giám sát và thông báo về các giao dịch đáng ngờ
6. **Grafana**: Dashboard trực quan hóa dữ liệu và giám sát hệ thống

## Yêu cầu hệ thống

- Docker và Docker Compose
- Ít nhất 8GB RAM dành cho Docker
- 10GB dung lượng đĩa trống

## Cài đặt và khởi chạy

### Bước 1: Clone repository

```bash
git clone https://github.com/yourusername/realtime_fraud_detection.git
cd realtime_fraud_detection
```

### Bước 2: Khởi động hệ thống

```bash
docker-compose up -d
```

Quá trình khởi động lần đầu có thể mất vài phút để tải các images và khởi tạo các dịch vụ.

### Bước 3: Kiểm tra trạng thái

```bash
docker-compose ps
```

## Các thành phần chính

### Data Generator

Tạo dữ liệu giao dịch giả lập và gửi đến Kafka. Dữ liệu bao gồm:
- ID giao dịch
- ID người dùng
- Số tiền giao dịch
- Loại tiền tệ
- Thời gian
- Vị trí
- ID người bán

### Spark Processor

Xử lý các giao dịch thời gian thực từ Kafka và:
- Áp dụng các quy tắc phát hiện gian lận (ví dụ: giao dịch > 30.000 được đánh dấu là nghi ngờ)
- Lưu trữ kết quả vào PostgreSQL
- Đánh chỉ mục dữ liệu trong Elasticsearch

### Alert Service

Giám sát các giao dịch được đánh dấu là đáng ngờ và cung cấp API để truy vấn cảnh báo.

## Truy cập các dịch vụ

- **Grafana**: http://localhost:3000 (user: admin, password: admin)
- **Alert Service API**: http://localhost:5000
  - `/health`: Kiểm tra trạng thái dịch vụ
  - `/alerts/recent`: Xem danh sách cảnh báo gần đây

## Kiểm tra và theo dõi dữ liệu

### Kiểm tra dữ liệu trong PostgreSQL

```bash
docker exec -it postgres_db psql -U user -d fraud_db -c "SELECT COUNT(*) FROM transactions_history;"
docker exec -it postgres_db psql -U user -d fraud_db -c "SELECT * FROM transactions_history LIMIT 5;"
```

### Kiểm tra dữ liệu trong Elasticsearch

```bash
curl -X GET "localhost:9200/transactions_index/_count"
curl -X GET "localhost:9200/transactions_index/_search?pretty&size=5"
```

## Giám sát và báo cáo

Đăng nhập vào Grafana (http://localhost:3000) để truy cập các dashboard có sẵn:
- Dashboard tổng quan giao dịch
- Dashboard theo dõi giao dịch đáng ngờ
- Dashboard theo dõi hiệu suất hệ thống

## Mở rộng và tùy chỉnh

### Điều chỉnh ngưỡng phát hiện gian lận

Thay đổi biến `RULE_THRESHOLD` trong file process_stream.py để điều chỉnh ngưỡng phát hiện giao dịch đáng ngờ.

### Thêm quy tắc phát hiện

Bạn có thể thêm các quy tắc phức tạp hơn vào hàm xử lý trong process_stream.py.

### Tùy chỉnh cảnh báo

Thêm các kênh thông báo như email hoặc webhook Slack trong alert_service.py.

## Dừng hệ thống

```bash
docker-compose down
```

Để giữ lại dữ liệu trong volumes khi dừng, sử dụng:
```bash
docker-compose stop
```

## Xử lý sự cố

### Logs

```bash
# Xem logs của dịch vụ cụ thể
docker logs spark-submit
docker logs postgres_db
docker logs alert_service
```

### Kiểm tra kết nối

```bash
# Kiểm tra kết nối giữa các dịch vụ
docker exec -it spark-submit nc -zv postgres_db 5432
docker exec -it alert_service curl -s elasticsearch:9200
```

## Đóng góp

Vui lòng đọc CONTRIBUTING.md để biết chi tiết về quy trình đóng góp vào dự án.

## Giấy phép

Dự án này được cấp phép theo MIT License - xem file LICENSE để biết thêm chi tiết.