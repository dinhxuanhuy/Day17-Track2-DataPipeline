# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đinh Xuân Huy  **Lớp:** Track 2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 15.5s
  run 2/3 … 15.3s
  run 3/3 … 15.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8622572a97    8622572a97    8622572a97   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✗  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  3/4 tiêu chí đạt
```

</details>

Tổng kết: **3 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline (hoặc Clear Task trên Airflow), số hàng trong `gold_training_set` bị nhân lên sau mỗi lần chạy (lên tới 38,750 hàng sau 3 lượt). Bảng đích bị lặp ticket (1,310 ticket có nhiều hơn 1 hàng). |
| **Nguyên nhân** | 1. Model incremental trong dbt thiếu `unique_key` và `incremental_strategy`, khiến dbt dùng chiến lược mặc định là `append` (`INSERT INTO`).<br>2. Nguồn CDC có bản ghi `op='u'` (1,310 bản ghi sửa đổi), một ticket tạo ngày D1 và sửa ngày D2 lọt qua điều kiện lọc `_ingested_at` ở cả 2 ngày nên bị chèn lặp.<br>3. DAG Airflow để `catchup=True` và thiếu giới hạn `max_active_runs`. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` vào `config()`.<br>- `dags/ai_training_pipeline.py`: Đổi thành `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: `8622572a97` (ổn định) · 0 ticket bị lặp |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu 455 hàng (chỉ đạt 8,645 / 9,100 hàng, thiếu ~5%). Dữ liệu chỉ thiếu ở 11 ngày cũ (08-03 đến 08-13), các ngày mới (08-14 đến 08-16) vẫn đủ 650 customer/ngày. |
| **P99 độ trễ đo được** | **2.73 ngày** *(chính xác: 2.7258 ngày, Max: 2.9447 ngày, tỷ lệ trễ > 1 ngày: 5.05%)* |
| **Lookback đã chọn** | 3 ngày — vì P99 độ trễ là 2.73 ngày (và Max là 2.94 ngày), lùi 3 ngày đảm bảo bao phủ toàn bộ dữ liệu đến muộn mà không lãng phí chi phí quét lại dữ liệu quá xa. |
| **Nguyên nhân** | 1. Có ~5.05% sự kiện bị trễ nạp vào kho từ 1 đến gần 3 ngày (`_ingested_at > event_time`).<br>2. Điều kiện incremental hiện tại `where event_date > (select max(event_date) from {{ this }})` chỉ lọc những sự kiện có ngày lớn hơn ngày lớn nhất đã có trong bảng đích. Khi một sự kiện cũ đến muộn (ví dụ `event_date = 08-12` nạp vào kho ngày `08-15`), `max(event_date)` trong target lúc đó đã là `08-14`, nên sự kiện bị loại bỏ hoàn toàn (`08-12 <= 08-14`) và không bao giờ được tính.<br>3. Thiếu cấu hình `unique_key` composite `['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` khiến việc mở rộng lookback window sẽ làm kết quả bị cộng dồn/lặp hàng khi tính lại partition cũ. |
| **Cách khắc phục** | - `dbt/models/gold/gold_feature_daily.sql`: Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` vào `config()`.<br>- Mở rộng điều kiện lọc incremental: `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng · checksum 3 lượt: `3db448685c` (ổn định ✓) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> - **P99 (2.73 ngày -> chọn lookback 3 ngày):** Bao phủ 99%–100% dữ liệu đến muộn thông thường trong khi giới hạn chi phí compute và I/O ở mức cố định, có thể dự đoán được ở mỗi lượt chạy hàng ngày (chỉ reprocess 3 ngày gần nhất).
> - **Max (hoặc outlier trong thực tế):** Nếu hệ thống có outlier trễ hàng chục ngày hoặc vài tháng (do thiết bị offline, sự cố mạng kéo dài), việc nới lookback theo `max` sẽ bắt **mọi lượt chạy hàng ngày** phải quét và tính toán lại một khối lượng dữ liệu khổng lồ trong quá khứ, lãng phí tài nguyên compute/I/O khi 99.9% dữ liệu đó không thay đổi. Với phần thiểu số cực trễ ngoài P99, giải pháp chuẩn trong kiến trúc Data Warehouse là chạy batch reconciliation/backfill định kỳ (weekly/monthly) thay vì trả chi phí scan lớn ở từng chu kỳ hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách khắc phục** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | |
| 2 | |
| 3 | |
