# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Thái Lương` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602932` |
| GitHub | `thailuong1008-coder` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04622` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Năm khác biệt cụ thể giữa `bronze.raw_telemetry_a` (3021 dòng, `_batch_id = 17f098d6bbb1`) và `bronze.raw_telemetry_b` (3234 dòng, `_batch_id = 844e983d0ded`):
1. **Tên các cột (schema drift)**: Nguồn A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; trong khi nguồn B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: Nguồn A ghi bằng **m/s** (ví dụ dòng đầu `speed_mps = 1.213`), còn nguồn B ghi bằng **km/h** (ví dụ dòng đầu `spd_kmh = 55.11`, cần chia `3.6` mới ra m/s).
3. **Định dạng thời gian**: Nguồn A lưu chuỗi chuẩn **ISO 8601 UTC** (`2025-10-07T08:00:00.014Z`), còn nguồn B lưu số nguyên **epoch milliseconds** (`1759933801509`).
4. **Đơn vị bàn đạp ga/phanh**: Nguồn A chuẩn hoá trong thang tỷ lệ `[0, 1]` (`gas = 0.028`, `brake = 0.000`), còn nguồn B lưu theo phần trăm `[0, 100]` (`throttle_pct = 2.0`, `brake_pct = 0.0`).
5. **Độ lệch đồng hồ thiết bị (`telemetry_clock_offset_ms`)**: Trong `bronze.video_files`, `drive_a` có offset `0 ms`, còn đồng hồ telemetry của `drive_b` chạy nhanh hơn đồng hồ video `1500 ms` (dòng đầu `1759933801509` tương ứng `14:30:01.509 UTC`, trong khi video bắt đầu lúc `14:30:00+00:00`).

Pipeline tách mỗi nguồn một bảng Bronze riêng và giữ mọi cột ở dạng chuỗi (`string`) vì nguyên tắc cốt lõi của lớp Bronze là **append-only, giữ nguyên bản dữ liệu gốc (schema-on-read)**. Nếu gộp và ép kiểu (`CAST`) ngay lúc ingest, bất kỳ sự thay đổi schema (schema drift) hoặc dòng dữ liệu bẩn/rỗng nào từ cảm biến cũng sẽ làm sập quá trình nạp hoặc làm mất dữ liệu gốc không thể khôi phục; giữ nguyên dạng text kèm cột lineage (`_batch_id`, `_row_no`) giúp ta luôn có thể sửa logic parse ở lớp Silver và chạy lại từ Bronze mà không cần thu thập lại dữ liệu.


## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

(trả lời ở đây)

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
