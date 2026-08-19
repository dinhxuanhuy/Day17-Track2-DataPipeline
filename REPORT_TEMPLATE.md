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
  run 1/3 … 13.7s
  run 2/3 … 14.9s
  run 3/3 … 14.8s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline (hoặc Clear Task trên Airflow), số hàng trong `gold_training_set` bị nhân lên sau mỗi lần chạy (lên tới 38,750 hàng sau 3 lượt). Bảng đích bị lặp ticket (1,310 ticket có nhiều hơn 1 hàng). |
| **Nguyên nhân** | 1. Model incremental trong dbt thiếu `unique_key` và `incremental_strategy`, khiến dbt dùng chiến lược mặc định là `append` (`INSERT INTO`).<br>2. Nguồn CDC có bản ghi `op='u'` (1,310 bản ghi sửa đổi), một ticket tạo ngày D1 và sửa ngày D2 lọt qua điều kiện lọc `_ingested_at` ở cả 2 ngày nên bị chèn lặp.<br>3. DAG Airflow để `catchup=True` và thiếu giới hạn `max_active_runs`. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` vào `config()`.<br>- `dags/ai_training_pipeline.py`: Đổi thành `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: `8dd7c98653` (ổn định) · 0 ticket bị lặp |

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
| **Triệu chứng** | Từ ngày 08-10, team backend đổi kiểu gửi cột `priority` từ số (`'1'`..`'4'`) sang nhãn chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`). Cột `priority` trong `silver_tickets` dùng `try_cast(... as integer)` dẫn đến 6,606 dòng bị `NULL` và 312 bản ghi mang giá trị sai lệch (`0`, `5`, `-1`, `P1`, `P2`, `unknown`, `''`, `NULL`) lọt qua contract. Model phân loại AI bị giảm mạnh độ chính xác. |
| **Nguyên nhân** | 1. **Schema Evolution không được xử lý:** Backend thay đổi cách biểu diễn dữ liệu nhưng pipeline chỉ dùng `try_cast`, vừa làm mất thông tin của nhãn chuỗi hợp lệ (biến thành NULL), vừa để lọt các số ngoài khoảng 1..4 (vì chúng cast được thành số).<br>2. **Thiếu Data Contract & Quarantine:** Chưa bật ràng buộc schema (`contract.enforced: true`) và thiếu vùng cách ly phân loại các bản ghi lỗi.<br>3. **Sai thứ tự Deduplication:** Nếu xếp hạng (`row_number()`) trước rồi mới lọc, những ticket có bản ghi mới nhất bị lỗi sẽ bị loại bỏ hoàn toàn cả ticket khỏi Silver (tụt từ 12,480 xuống 12,168 ticket). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Nhóm 1 (Số hợp lệ - 6,846 bản ghi):** `'1'`, `'2'`, `'3'`, `'4'` ➔ Đúng contract ban đầu, giữ nguyên.<br>2. **Nhóm 2 (Nhãn chuỗi / Schema evolution - 7,142 bản ghi):** `'urgent'`, `'high'`, `'medium'`, `'low'` ➔ Ánh xạ về số nguyên 1..4 (`urgent→1`, `high→2`, `medium→3`, `low→4`).<br>3. **Nhóm 3 (Dữ liệu hỏng thật - 312 bản ghi):** `'0'`, `'5'`, `'-1'`, `'P1'`, `'P2'`, `'unknown'`, `''`, `NULL` ➔ Trả về `NULL` để chuyển vào `quarantine_tickets`. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Dùng `CASE WHEN` phân loại 3 nhóm; ánh xạ nhóm 2 về 1..4; nhóm 3 trả về `NULL`; bổ sung `priority_reject_reason`.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi `priority_clean is null` **trước** khi đánh số `row_number()`, bảo toàn đủ 12,480 ticket.<br>- `dbt/models/silver/quarantine_tickets.sql`: Đổi điều kiện thành `where {{ normalize_priority('priority_raw') }} is null`.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và thêm tests `not_null`, `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets` đủ 12,480 ticket · `silver_tickets.priority ∈ 1..4, không NULL` = sạch · `dbt test` = 11/11 pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn ở tầng Silver, KHÔNG chặn ở tầng Bronze:**
>    - Tầng Bronze tuân theo nguyên tắc *Immutable Raw Ingestion* (bảo tồn nguyên vẹn dữ liệu thô từ nguồn). Nếu từ chối bản ghi ngay tại Bronze, dữ liệu bị mất vĩnh viễn (data loss). Khi đó, kỹ sư mất dấu vết lịch sử (audit trail), không thể điều tra nguyên nhân gốc rễ, và không thể reprocess/backfill khi backend sửa lỗi hoặc cung cấp rule ánh xạ mới.
> 2. **Vì sao KHÔNG để pipeline dừng khi gặp bản ghi lỗi:**
>    - *Kiểm soát phạm vi ảnh hưởng (Blast Radius):* Trong chu kỳ vận hành, chỉ có 312 bản ghi CDC bị lỗi trên tổng số hơn 14,300 bản ghi CDC, hơn 130,000 sự kiện và 31,200 chunk văn bản. Nếu dừng cả pipeline, 99.9% dữ liệu hoàn toàn hợp lệ của toàn bộ khách hàng khác sẽ bị đình trệ, làm gián đoạn toàn bộ hệ thống RAG Index và AI Agent phục vụ kinh doanh.
>    - Áp dụng mô hình **Quarantine (Cách ly)** giúp cô lập các bản ghi lỗi vào hàng đợi riêng để người trực xử lý mà vẫn đảm bảo tính sẵn sàng (high availability) và thông suốt cho toàn bộ pipeline.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính Idempotent của các model Incremental: `unique_key`, `incremental_strategy` (`delete+insert` / `merge`), và cấu hình DAG (`catchup=False`, `max_active_runs=1`) để tránh nhân bản dữ liệu khi retry/clear task. |
| 2 | Kiểm tra phân bố độ trễ nạp dữ liệu (`_ingested_at` vs `event_time`) và cấu hình Lookback Window phù hợp (dựa trên P99) kèm composite `unique_key` để không bỏ sót late-arriving data. |
| 3 | Kiểm tra Data Contract (`contract: enforced: true`), các bộ test ràng buộc miền giá trị (`not_null`, `accepted_values`), và cơ chế Quarantine phân luồng bản ghi lỗi độc lập với pipeline chính. |
