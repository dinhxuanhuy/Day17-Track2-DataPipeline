# Báo cáo Sự Cố & Khắc Phục Lệnh `make setup`

**Dự án:** LAB 17 — Data Pipeline Engineering  
**Hệ điều hành:** Linux x86_64  
**Phiên bản Python:** Python 3.14.4  
**File ảnh hưởng:** [Makefile](file:///home/transcend/Day17-Track2-DataPipeline/Makefile)  

---

## 1. Hiện tượng sự cố (Symptom)

Khi thực hiện khởi tạo môi trường cho lab bằng lệnh `make setup`, quá trình thực thi bị dừng đột ngột với mã lỗi **127**:

```text
transcend@Transcend-14:~/Day17-Track2-DataPipeline$ make setup
/bin/bash: line 1: .venv/bin/pip: No such file or directory
make: *** [Makefile:24: setup] Error 127
```

---

## 2. Phân tích nguyên nhân gốc rễ (Root Cause Analysis)

### 2.1. Thiết kế ban đầu trong `Makefile`

Target `setup` và các biến môi trường ban đầu được định nghĩa như sau:

```makefile
VENV    := .venv
PY      := $(VENV)/bin/python
PIP     := $(VENV)/bin/pip
DBT     := $(VENV)/bin/dbt

setup:  ## venv + thư viện + sinh dữ liệu (chạy một lần)
	@test -d $(VENV) || python3 -m venv $(VENV)
	@$(PIP) install -q --upgrade pip
	@$(PIP) install -q -r requirements.txt
	@$(PY) seed/generate.py
```

### 2.2. Trình tự thực thi và cơ chế gây lỗi

1. **Bước 1 — Tạo Virtual Environment:**
   - Câu lệnh `@test -d $(VENV) || python3 -m venv $(VENV)` đã chạy thành công và tạo ra thư mục `.venv`.
2. **Bước 2 — Cấu trúc binary trong `.venv/bin/` trên Python 3.14:**
   - Trên phiên bản Python 3.14, module `venv` (thông qua `ensurepip`) chỉ khởi tạo các file thực thi:
     - `pip3`
     - `pip3.14`
     - `python` -> `python3`
     - `python3`
     - `python3.14`
   - **Không tồn tại** file nhị phân có tên là `pip` (không có hậu tố số).
3. **Bước 3 — Lỗi tìm file thực thi (Error 127):**
   - Biến `PIP` được gán cứng giá trị `$(VENV)/bin/pip` (tương đương `.venv/bin/pip`).
   - Khi `make` chạy `@$(PIP) install -q --upgrade pip`, shell `/bin/bash` tìm đường dẫn `.venv/bin/pip` nhưng không thấy, do đó trả về lỗi `Error 127: No such file or directory`.

---

## 3. Giải pháp khắc phục (Resolution)

Theo chuẩn khuyến nghị chính thức của Python Packaging (PEP / Python standard guidelines), không nên gọi trực tiếp file thực thi `pip` theo đường dẫn tĩnh mà nên gọi thông qua flag `-m pip` của Python interpreter (`python -m pip`). 

Cách tiếp cận này đảm bảo module `pip` luôn được định vị chính xác trong đúng môi trường ảo, tương thích với mọi phiên bản Python và hệ điều hành.

### Thay đổi trong [Makefile](file:///home/transcend/Day17-Track2-DataPipeline/Makefile#L4):

```diff
  SHELL   := /bin/bash
  VENV    := .venv
  PY      := $(VENV)/bin/python
- PIP     := $(VENV)/bin/pip
+ PIP     := $(PY) -m pip
  DBT     := $(VENV)/bin/dbt
```

---

## 4. Kết quả kiểm chứng (Verification)

Sau khi chỉnh sửa, thực thi lại lệnh `make setup`:

```bash
make setup
```

**Kết quả thực tế:**
```text
  [seed] dựng bảng nguồn…
  [seed] ticket: 12735 tạo / 255 xoá / 12480 sống / 998 sửa / 312 sai kiểu
  [seed] ghi seed/tickets_cdc.jsonl…
  [seed] ghi seed/events.jsonl…
  [seed] ghi seed/transcripts.jsonl…

  CDC rows        :    14,300
  events          :   130,683
  transcripts     :     5,200
  expected/       : {"gold_training_set": 12480, "gold_feature_daily": 9100, "gold_doc_chunks": 31200, "quarantine_tickets": 312}
  xong sau 1.4s

  xong. Bước tiếp theo:  make pipeline  rồi  make verify
```

- Virtual environment `.venv` hoạt động bình thường.
- Đã cài đặt đầy đủ các gói thư viện (`duckdb`, `dbt-core`, `dbt-duckdb`,...).
- Toàn bộ dữ liệu seed và baseline đã được sinh thành công.
