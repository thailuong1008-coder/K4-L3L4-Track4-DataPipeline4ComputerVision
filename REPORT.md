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

Trong `stg_telemetry_a.sql` và `stg_telemetry_b.sql`, pipeline chuẩn hoá hai nguồn về cùng một schema thống nhất:
- **Đơn vị & tên cột**: Nguồn B đổi `spd_kmh / 3.6` thành `speed_mps` (m/s), đổi `throttle_pct / 100` thành `gas` và `brake_pct / 100` thành `brake`, đồng thời ánh xạ `hdg -> heading_deg`, `latitude -> lat`, `longitude -> lon`, `altitude -> alt_m`, `ax -> acc_x`, `ay -> acc_y`, `steering -> steer`.
- **Kiểu thời gian & độ lệch đồng hồ**: Nguồn A ép kiểu `cast(ts_utc as timestamptz)`, còn nguồn B join với `bronze.video_files` để lấy `telemetry_clock_offset_ms = 1500` rồi tính `to_timestamp((cast(timestamp_ms as bigint) - offset_ms) / 1000.0)` để khử độ lệch 1,5 giây.
- **Bằng chứng trong kết quả bước 2**: Bảng `silver.silver_telemetry` có `bat_dau` của `drive_b` là `2025-10-08 14:30:00.009000+00:00` (khớp sát thời điểm bắt đầu video `14:30:00+00:00`, thay vì `14:30:01.509` khi chưa trừ offset), tốc độ tối đa hợp lý (`drive_a`: `71.0 km/h`, `drive_b`: `100.0 km/h`), và tổng số dòng bảo toàn tuyệt đối: `drive_a` có `2968 sạch + 53 cách ly = 3021`, `drive_b` có `3181 sạch + 53 cách ly = 3234`.

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Dòng lỗi được đưa vào bảng quarantine `silver.silver_telemetry_rejects` (tổng 106 dòng) kèm `reject_reason` thay vì bị xoá im lặng để **đảm bảo tính kiểm toán (auditability), không làm mất dấu dữ liệu**, giúp kỹ sư theo dõi tỷ lệ lỗi cảm biến theo thời gian và khôi phục lại nếu phát hiện quy tắc lọc bị đặt sai.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Phân biệt `warn` và `error`**: Test `max_gap_ms` (kiểm tra khoảng trống giữa 2 mẫu liên tiếp `> 1000 ms`) được cấu hình `severity: warn` vì hiện tượng cảm biến GPS/telemetry rớt tín hiệu tạm thời vài giây (ví dụ khi qua hầm) là khuyết tật thực tế đã biết và chấp nhận được (xử lý tiếp ở bước join khung hình bằng `sync_tol_ms`), không nên làm dừng toàn bộ pipeline. Ngược lại, `in_range` (`0..70 m/s`) là `error` (mặc định) vì tốc độ âm hoặc vượt quá `70 m/s` (`252 km/h`) là dữ liệu vật lý vô lý/hỏng; nếu lọt xuống Gold sẽ làm sai lệch nhãn huấn luyện mô hình.
- **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**: Khi nới biến `max_speed_mps` lên `1000`, `dbt test` trả về `PASS=5 WARN=1 ERROR=1 SKIP=0` (`gate_failed: true`). Test **`in_range_silver_telemetry_speed_mps__70__0`** đã bắt được đúng **6 dòng** vi phạm (3 dòng của `drive_a` và 3 dòng của `drive_b` có tốc độ vô lý lọt từ quarantine vào `silver_telemetry`). Khi khôi phục luật gốc, kết quả trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.
- **Vai trò của cổng chất lượng (Quality Gate)**: Cổng chất lượng đóng vai trò chốt chặn độc lập (defense-in-depth) giữa bước biến đổi và bước xuất bản: ngay cả khi logic lọc/biến đổi ở code SQL bị sửa sai hoặc nới lỏng nhầm, cổng `dbt test` vẫn chặn đứng dữ liệu bẩn (`ERROR=1`), ngăn các task downstream (`dbt_run_gold`, `export_dataset`, `publish_warehouse`) chạy tiếp.


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
