# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Thái Lương` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602932` |
| GitHub | `thailuong1008-coder` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04622` |

## Kết quả cuối

- `dataset_version` của bạn: `a63c8ed25319`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `655 / 192 / 172` (tổng `1019` khung hình từ `26` trip: `18` trip train, `4` trip val, `4` trip test; `drive_a = 436` khung, `drive_b = 583` khung)
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

- **Số liệu từ `idempotency.json` (trên partition `trip-04622` gồm `60` khung hình)**:
  - Khi dùng Delta `overwrite` có điều kiện `predicate="trip_id = 'trip-04622'"`: Số dòng của bảng `silver.frames` trước khi chạy lại là **`1246` dòng** (`fingerprint_before = 6c7480dfde7e`), và sau **2 lần chạy lại liên tiếp** vẫn giữ nguyên **`1246` dòng** (`fingerprint_after = 6c7480dfde7e`, `unchanged: true`).
  - Khi ghi kiểu `append` sai cách: Chạy 2 lần trên `trip-04622` (`60` khung) sinh ra **`120` dòng** (`append_variant_rows_after_two_runs = 120`), làm dữ liệu bị nhân đôi gấp 2 lần.
- **Vì sao Delta `overwrite` theo partition an toàn hơn "xoá rồi ghi lại" (DELETE + INSERT)**:
  - Trong Delta Lake, thao tác `overwrite` với `predicate` là một **giao dịch ACID nguyên tử (atomic transaction)** dựa trên transaction log (`_delta_log`): file Parquet mới được ghi hoàn tất trước, sau đó một commit duy nhất mới vừa đánh dấu `remove` các file cũ của partition đó vừa `add` file mới.
  - Nếu làm thủ công kiểu "xoá trước rồi ghi lại sau" mà tiến trình bị sập giữa chừng (OOM, mất mạng, lỗi giải mã video), dữ liệu của trip đó đã bị xoá mất trắng và các tiến trình đọc đồng thời sẽ thấy bảng bị thiếu dữ liệu; ngược lại với Delta `overwrite`, nếu sập giữa chừng thì commit chưa được ghi vào log, bảng vẫn giữ nguyên trạng thái hợp lệ ở version trước đó (snapshot isolation) và hỗ trợ time travel.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Khác biệt giữa khung đen (`is_black`) và khung tối (`is_dark`)**:
  - **Khung đen** (`brightness < 12.0`, `drive_a` có `6` khung, `drive_b` có `4` khung): Là lỗi hỏng hóc cảm biến/rớt khung hình hoàn toàn không mang thông tin thị giác, nên pipeline **loại bỏ (drop)** khi sang lớp Gold.
  - **Khung tối** (`12.0 <= brightness < 40.0`, `drive_a` có `4` khung, `drive_b` có `61` khung): Là điều kiện vận hành thực tế (xe đi qua đường hầm, bóng râm hoặc trời tối), nên pipeline **gắn cờ `is_dark = true` nhưng vẫn giữ lại (`toi_giu_lai`)** để huấn luyện. Tôi **hoàn toàn đồng ý** với cách xử lý này: nếu loại bỏ cả khung tối thì mô hình xe tự lái sẽ bị thiên lệch (bias) chỉ biết chạy ban ngày đủ sáng và thất bại khi vào hầm/ban đêm; còn cờ `is_dark` giúp kiểm tra độ cân bằng dữ liệu theo điều kiện ánh sáng.
- **Lựa chọn ngưỡng dHash (`threshold.json`) và mối liên hệ với tốc độ xe**:
  - Tôi chọn **ngưỡng `2` bit (`dup_hamming = 2`)** (loại `149/602` khung ở `drive_a` và `53/644` khung ở `drive_b`). Ngưỡng `0` quá khắt khe vì nhiễu nén JPEG/nhiễu cảm biến tự nhiên làm hai khung đứng yên liên tiếp vẫn lệch 1–2 bit (ở `drive_a` camera đứng hình 6 giây = 12 khung nhưng chỉ bắt được nếu cho phép sai số nhỏ); ngược lại ngưỡng `6` quá lỏng, loại tới `363/602` khung (`60,3%`) ở `drive_a` và `244/644` khung (`37,9%`) ở `drive_b`, làm mất cả những khung cảnh xe đang di chuyển thật.
  - **Phụ thuộc vào tốc độ xe**: Ở tốc độ lấy mẫu cố định `2 fps`, khi xe chạy chậm hoặc dừng đèn đỏ (như `drive_a`), cảnh vật giữa hai khung liên tiếp cách nhau `0,5 giây` thay đổi rất ít nên khoảng cách Hamming dHash nhỏ (nhiều khung trùng); khi xe chạy nhanh (như `drive_b` lên tới `100 km/h`), cảnh thay đổi nhanh nên ít khung trùng hơn (`53` so với `149`). Do đó pipeline nâng cao thực tế nên kết hợp ngưỡng dHash động theo `speed_mps`.


## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

- **Số liệu từ `leakage.json`** (trên tổng số **`1019` khung hình** hợp lệ thuộc **`26` trip**):
  - Khi chia ngẫu nhiên **theo khung hình** (`per-frame split` theo tỷ lệ 70/15/15): Có tới **`26/26` trip** nằm ở nhiều hơn một split (`per_frame_split_trips_straddling = 26`), và cả **`26/26` trip** đều có khung hình xuất hiện đồng thời ở cả tập `train` lẫn tập `test` (`per_frame_split_trips_in_both_train_and_test = 26`).
  - Khi chia **theo trip** (`per-trip split` ở bảng `gold.gold_dataset_frames` của pipeline): Có **`0` trip** bị chia cắt (`per_trip_split_trips_straddling = 0`), mỗi trip chỉ thuộc duy nhất một trong ba tập (`train = 18` trip, `val = 4` trip, `test = 4` trip).
- **Vì sao làm điểm kiểm thử đẹp một cách giả tạo (Data Leakage)**: Video được lấy mẫu `2 fps` (hai khung liên tiếp chỉ cách nhau `0,5 giây`), nên các khung hình kế cận trong cùng một trip có bối cảnh nền, điều kiện ánh sáng, thời tiết, mặt đường và vận tốc gần như giống hệt nhau. Nếu chia theo khung hình, khung tại giây `t` vào `train` còn khung tại `t + 0.5s` vào `test`; mô hình chỉ cần học thuộc lòng (memorize) đặc trưng cảnh cụ thể thay vì học quy luật tổng quát mà vẫn đạt độ chính xác rất cao trên tập `test`, nhưng khi đem ra xe chạy thực tế ở tuyến đường/cảnh mới thì chất lượng sụp đổ.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **Vì sao `dataset_version` là băm nội dung (content-addressed hash)**: Trong `pipeline/publish.py`, `dataset_version` được tính bằng `sha256("\n".join(f"{frame_id}|{split}" ...))[:12]`. Dùng băm nội dung đảm bảo **tính tất định (deterministic) và bất biến**: cùng một tập khung hình và nhãn split sẽ luôn sinh ra đúng một mã phiên bản duy nhất bất kể chạy vào ngày giờ nào hay chạy lại bao nhiêu lần (khác với timestamp mỗi lần chạy sinh một thư mục mới gây rác, hoặc số đếm tự tăng phụ thuộc vào trạng thái ngoài).
- **Kết quả hai lần chạy liên tiếp**: Khi chạy `step 4` và chạy lại `cli export-dataset` liên tiếp, cả hai lần đều trả về cùng một `dataset_version = a63c8ed25319` và cùng `manifest_sha256 = de2ce2276fe55449f1378b1c819927679bef63e76a2e65964e00448ae999b468`.
- **Vì sao khác bạn bè dù cùng dữ liệu**: Trong `gold_dataset_frames.sql`, thứ tự các trip trước khi gán `train/val/test` được sắp xếp theo `md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`. Với `LAB_STUDENT_ID = 2A202602932`, hàm băm MD5 tạo ra một hoán vị thứ tự trip riêng biệt của tôi (`train: 655`, `val: 192`, `test: 172` khung), dẫn đến chuỗi ghép `frame_id|split` khác với các bạn có MSSV khác và sinh ra `dataset_version = a63c8ed25319` duy nhất cho MSSV của tôi.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Theo mục `lineage` và `rules` trong `s3://lake/gold/datasets/a63c8ed25319/manifest.json`, dataset `a63c8ed25319` (`1019` khung hình: `436` khung từ `drive_a`, `583` khung từ `drive_b`) được tạo từ:
- **Hai file video gốc (kiểm chứng bằng SHA-256)**:
  - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- **Phiên bản (commit version) cụ thể của các bảng Delta thượng nguồn**:
  - Lớp Bronze: `bronze.raw_telemetry_a` (version `0`), `bronze.raw_telemetry_b` (version `0`), `bronze.trips_raw` (version `0`), `bronze.video_files` (version `1`).
  - Lớp Silver: `silver.frames` (version `25` khi chạy mới toàn bộ 26 trip từ `0..25`, hoặc version `27` sau 2 lần chạy lại ở thí nghiệm idempotency), `silver.telemetry` (version `0`), `silver.frames_flagged` (version `0`), `silver.frame_telemetry` (version `0`).
- **Làm sao tái tạo lại đúng dataset này sau 3 tháng**: Nhờ tính năng **Delta Time Travel** kết hợp với `manifest.json`, ta chỉ cần: (1) đối chiếu SHA-256 của video đầu vào, (2) truy vấn đúng số `version` của các bảng Delta trong `lineage`, và (3) áp dụng lại đúng tập luật `rules` (`drop: ["is_black", "is_blurry", "is_near_dup", "not sync_ok"]`, `split: "per trip, stratified by drive, md5 order"` với `LAB_STUDENT_ID = 2A202602932`) là sẽ thu lại chính xác `1019` khung hình có `dataset_version = a63c8ed25319`.


## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

- **Khi đặt `LAB_FAIL_ONCE=trip-04622` và chạy DAG `day09_video_pipeline` (`run_crash_recovery`)**:
  - Có đúng **`1` task chuyển đỏ (`failed`)**: `silver_frames` tại **`map_index = 12`** (tương ứng `trip-04622`), trong khi `25/26` task `silver_frames` còn lại và `4` task đầu nguồn (`bronze_drive_a`, `bronze_drive_b`, `bronze_trips`, `trip_commands`) đều xanh (`29` task `success`).
  - Có đúng **`7` task phía sau chuyển sang `upstream_failed` (màu cam)**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` — dữ liệu chưa đầy đủ bị chặn lại, không đi tiếp.
- **Thao tác khôi phục (Clear) và kết quả**:
  - Tôi chỉ **Clear đúng task đỏ `silver_frames[12]` (`trip-04622`) kèm 7 task downstream** của nó: chỉ `1` partition `trip-04622` chạy lại (`try_number = 2`) và `7` task phía sau chạy tiếp; `25` trip đã thành công không phải giải mã lại từ đầu.
  - **Dữ liệu không bị nhân đôi** và **`dataset_version` không đổi**: Bảng `silver.frames` vẫn giữ đúng `1246` khung hình, `gold.gold_dataset_frames` giữ đúng `1019` khung hình (`655/192/172`), `dataset_version` vẫn là **`a63c8ed25319`**, và `verify` đạt tuyệt đối **`27/27 checks passed`**.
  - **Bằng chứng**: Ảnh chụp `report/screenshots/airflow_crash_log.png` (hiển thị rõ `Task: silver_frames [] | Map Index: 12` và dòng log đỏ `RuntimeError: simulated crash while extracting trip-04622; run it again` cùng 7 ô cam `upstream_failed` bên trái) và ảnh `report/screenshots/airflow_graph.png` (toàn bộ 37 task instances chuyển xanh hoàn toàn sau khi clear).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng từ 2 drive (~27 MB video, ~6200 dòng telemetry) lên **500 xe mỗi ngày** (hàng chục TB video 7 camera và hàng tỷ bản ghi cảm biến mỗi ngày), **3 thành phần sẽ vỡ đầu tiên** gồm:
1. **Giải mã và trích xuất video (`pipeline/frames.py` chạy FFmpeg CPU tuần tự + tải nguyên file `raw.mp4` về từng worker)**: Ở bước `extract_trip`, mỗi trip đều tải lại toàn bộ file video của drive từ S3 về ổ tạm rồi chạy FFmpeg trên CPU với `LocalExecutor` (`max_active_tis_per_dagrun=1`). Với 500 xe/ngày, băng thông mạng S3 và CPU một máy sẽ nghẽn cổ chai ngay lập tức. **Thay bằng (Slide 12, 23)**: Cắt chunk video hoặc đọc theo byte-range/streaming trực tiếp tại biên (edge) hoặc dùng cụm worker phân tán có GPU tăng tốc phần cứng (**NVIDIA NVDEC / Ray Data / Spark / KubernetesExecutor**) để decode song song.
2. **Engine xử lý dữ liệu đơn máy (`DuckDB` lưu trên một file `/opt/work/lakehouse.duckdb`)**: DuckDB là in-process OLAP cực nhanh trên một node cho dữ liệu vừa và nhỏ, nhưng không hỗ trợ ghi đồng thời từ nhiều máy (single-writer lock trên file `.duckdb`) và sẽ tràn RAM/ổ đĩa khi join ASOF hàng tỷ dòng telemetry với hàng trăm triệu khung hình. **Thay bằng (Slide 22, 23)**: Chuyển sang engine tính toán phân tán như **Apache Spark / Databricks (dbt-spark)** hoặc **Trino / ClickHouse** đọc/ghi trực tiếp trên Delta Lake phân vùng theo `date / drive_id`.
3. **Cách nạp telemetry theo file CSV batch thủ công & kho phục vụ (`publish_warehouse` copy nguyên bảng bằng `CREATE OR REPLACE TABLE` sang Postgres)**: Với 500 xe gửi telemetry liên tục, việc gom file CSV và ghi đè toàn bộ bảng mart sang một instance Postgres đơn lẻ sẽ làm treo I/O. **Thay bằng (Slide 12, 22)**: Dùng luồng ingestion thời gian thực (**Kafka / Redpanda + Spark Structured Streaming / Flink**) ghi incremental vào Delta Lake, và dùng dbt **incremental models** kết hợp warehouse phân tán (ClickHouse / BigQuery / Snowflake) hoặc truy vấn trực tiếp Lakehouse qua Trino.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT
    drive_id,
    trip_id,
    lighting,
    conditions,
    frames AS total_frames,
    frames_kept,
    (frames - frames_kept) AS frames_dropped,
    ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 1) AS drop_rate_pct,
    avg_speed_kmh,
    split
FROM marts.mart_trip_summary
ORDER BY drop_rate_pct DESC, frames_dropped DESC;
```

**Ý nghĩa và kết quả phân tích vận hành**:
- Câu truy vấn xếp hạng các trip theo **tỷ lệ khung hình bị loại (`drop_rate_pct`)** để tìm ra chuyến đi nào hao hụt dữ liệu nhiều nhất và đối chiếu với tốc độ trung bình (`avg_speed_kmh`).
- Kết quả cho thấy **`trip-04622` (`drive_a`)** có tỷ lệ bị loại cao nhất toàn bộ dữ liệu: **`56,7%`** (bị loại `34/60` khung, chỉ giữ lại `26` khung, hoàn toàn do cờ `is_near_dup` từ đoạn camera bị đứng hình 6 giây), tiếp theo là `trip-03904` (`50,0%`, loại `11/22` khung) và `trip-02086` (`46,7%`, loại `28/60` khung khi xe đi chậm `17,6 km/h`) và `trip-08888` (`40,0%`, loại `24/60` khung khi xe bò chậm `6,5 km/h`).
- Nhìn tổng thể, `drive_a` có tỷ lệ khung bị loại cao hơn hẳn `drive_b` (`27,6%` so với `9,5%`) vì `drive_a` vừa gặp sự cố đứng hình camera, vừa có nhiều đoạn chạy chậm trong đô thị làm sinh ra nhiều khung hình gần trùng (`near-duplicate`).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Điều bất ngờ nhất**: Chỉ với các tín hiệu thị giác cực nhẹ không cần mô hình học sâu (độ sáng trung bình `luma`, phương sai Laplacian `blur_score`, và băm 64-bit `dHash`), kết hợp với `ASOF JOIN` theo event time và giao dịch ACID của Delta Lake, pipeline đã tự động bắt chính xác 100% khung đen, khung mờ, camera đứng hình và chống rò rỉ dữ liệu (`0/26` trip bị cắt ngang so với `26/26` khi chia theo frame).
- **Điều còn muốn tìm hiểu thêm**: Cách tự động điều chỉnh ngưỡng `dup_hamming` hoặc tần số lấy mẫu `SAMPLE_FPS` phụ thuộc vào vận tốc thực tế (`speed_mps`) để không lấy dư khung khi xe dừng đèn đỏ mà vẫn lấy đủ dày khi xe vào cua tốc độ cao.
- **Một lỗi dữ liệu mà pipeline hiện tại chưa bắt được**: Nếu camera bị **lệch góc lắp đặt (camera miscalibration/occlusion một phần)** như bị bám bùn đất một góc kính, bị lóa nắng ngược sáng cục bộ (glare), hoặc **telemetry bị kẹt giá trị hợp lệ (stuck sensor)** (ví dụ cảm biến tốc độ bị treo ở đúng `15.0 m/s` suốt 30 giây), thì cả độ sáng trung bình, độ nét toàn ảnh lẫn kiểm tra `in_range(0..70 m/s)` đều vẫn báo PASS dù nhãn huấn luyện đã bị sai lệch.

