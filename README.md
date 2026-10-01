# PySpark: UDF / Pandas UDF / UDTF

Bài thực hành so sánh Python UDF, Pandas UDF, UDTF với các hàm built-in của Spark.

## Cấu trúc
```
.
├── data/customers.csv     # customer_id, customer_name, province, amount, note
├── udf_practice.py        # toàn bộ 4 yêu cầu
├── performance_practice.py # mở rộng: explain, Arrow, shuffle, cache, skew
├── requirements.txt
└── output/                # sinh ra khi chạy (Parquet)
    ├── customers_processed
    └── customer_tags
```

## Chạy
```bash
pip install -r requirements.txt   # cần Java 11/17/21 và PySpark >= 3.5
python udf_practice.py
python performance_practice.py        # hoặc: python performance_practice.py C D
SCALE=0.2 python performance_practice.py   # giảm dữ liệu nếu máy yếu
```

## Kết quả
- `customers_processed`: thêm `customer_name_clean`, `customer_segment`, `amount_with_rate`.
- `customer_tags`: `customer_id | tag` (mỗi tag một dòng, tách bằng UDTF).

Quy tắc phân loại: `VIP` ≥ 10.000.000; `STANDARD` ≥ 3.000.000; còn lại `BASIC`.

---

## Yêu cầu 2 – UDF vs built-in

| Tiêu chí | Python UDF | Built-in (`trim`, `upper`, `when/otherwise`) |
|---|---|---|
| Cách chạy | Python worker, mỗi dòng serialize JVM ↔ Python | Chạy thẳng trong JVM |
| Catalyst optimizer | Xem UDF là "hộp đen", không tối ưu/pushdown | Tối ưu đầy đủ, codegen |
| Physical plan | `BatchEvalPython` (pickle) hoặc `ArrowEvalPython` (Arrow; mặc định từ Spark 4.0) | Chỉ `Project` |
| Độ dài code | Dài hơn (hàm + decorator + returnType) | Ngắn, khai báo |
| Xử lý NULL | Phải tự kiểm tra `None` | Tự lan truyền NULL |
| Hiệu năng (1 triệu dòng, máy local) | ~2.3s | ~0.2s |

- **Dễ đọc hơn:** built-in cho logic đơn giản (chuỗi, điều kiện) vì đọc như SQL, không cần thêm hàm Python. UDF chỉ dễ đọc hơn khi logic rất phức tạp (nhiều nhánh, regex đặc thù).
- **Nên ưu tiên built-in:** khi Spark đã có hàm tương đương (chuỗi, ngày giờ, điều kiện, collection, JSON...), dữ liệu lớn, cần tận dụng optimizer/pushdown, hoặc job chạy thường xuyên.

## Yêu cầu 3 – Python UDF vs Pandas UDF

| | Python UDF | Pandas UDF |
|---|---|---|
| Đơn vị xử lý | Từng dòng (scalar) | Cả batch (`pd.Series`) |
| Truyền dữ liệu | Pickle, từng dòng | Apache Arrow, columnar, ít copy |
| Cách viết | `def f(x): return x * 1.1` | `def f(s: pd.Series) -> pd.Series: return s * 1.1` |
| Tốc độ (1 triệu dòng) | ~2.3s | ~0.7s |

Pandas UDF nhanh hơn vì Arrow truyền dữ liệu theo cột với chi phí serialize thấp, và phép tính được vector hóa bằng numpy/pandas thay vì vòng lặp Python từng dòng. Nó phù hợp khi cần thư viện pandas/numpy/sklearn hoặc logic không viết được bằng built-in. Nếu built-in làm được (như `amount * 1.1`) thì built-in vẫn nhanh nhất (~0.2s).

## Yêu cầu 4 – UDTF

UDTF `SplitTags` nhận `(customer_id, tags)` và `yield` nhiều dòng `(customer_id, tag)`:

```
1 | spark,python,etl   →   1 | spark
                            1 | python
                            1 | etl
```
Gọi qua SQL: `FROM t, LATERAL split_tags(t.customer_id, t.tags)`. Trường hợp đơn giản như tách chuỗi, `explode(split(...))` cho kết quả giống hệt (đã kiểm tra trong code) và nhanh hơn.

---

## Trả lời cuối bài

**1. UDF là gì?**
User Defined Function: hàm do người dùng tự viết (Python/Scala/Java) rồi đăng ký vào Spark để dùng trong DataFrame/SQL khi built-in không đáp ứng được. Nhận giá trị của mỗi dòng và trả về **một** giá trị.

**2. UDTF khác UDF như thế nào?**
UDF: 1 dòng vào → **1 giá trị** ra (thêm/biến đổi cột). UDTF: 1 dòng vào → **0, 1 hoặc nhiều dòng** ra (mỗi dòng có thể nhiều cột), nên thay đổi số dòng của kết quả. UDTF dùng trong mệnh đề `FROM` (thường với `LATERAL`), UDF dùng trong `SELECT`/`withColumn`/`WHERE`. Ví dụ UDTF: tách `"spark,python,etl"` thành 3 dòng.

**3. Khi nào nên dùng built-in thay vì UDF?**
Khi Spark đã có hàm làm được việc đó (`trim`, `upper`, `regexp_replace`, `when/otherwise`, `split`, `explode`, hàm ngày giờ...), khi dữ liệu lớn, khi cần Catalyst tối ưu (predicate pushdown, codegen), và khi muốn code ngắn gọn, ít lỗi NULL. Chỉ dùng UDF khi logic thật sự không biểu diễn được bằng built-in (dùng thư viện ngoài, thuật toán tùy biến).

**4. Python UDF và Pandas UDF khác nhau ở điểm nào?**
Python UDF chạy từng dòng, serialize bằng pickle, chậm do overhead lớn. Pandas UDF chạy theo batch (`pd.Series`/`DataFrame`), truyền dữ liệu qua Apache Arrow và vector hóa bằng pandas/numpy nên thường nhanh hơn nhiều, nhất là với phép tính số học và thư viện khoa học dữ liệu.

**5. Cùng một logic viết được bằng built-in thì chọn cách nào?**
Chọn **built-in**. Lý do: chạy trong JVM (không tốn chi phí chuyển dữ liệu sang Python), được Catalyst tối ưu, nhanh nhất (benchmark: 0.2s so với 0.7s Pandas UDF và 2.3s Python UDF), code ngắn, dễ bảo trì và dễ debug qua `explain()`. Thứ tự ưu tiên: **built-in → Pandas UDF → Python UDF → UDTF** (UDTF chỉ khi cần sinh nhiều dòng với logic phức tạp mà `explode` không đáp ứng).

---

## Mở rộng: Performance, Shuffle, Cache, Data Skew

Script: `performance_practice.py` (chia 5 phần A–E). Số liệu dưới đây đo trên máy sandbox **1 CPU, 4GB RAM, Spark 4.2, `SCALE=0.2`**, chỉ có giá trị so sánh tương đối; chạy lại trên máy bạn để lấy số của riêng mình.

### A. Đọc `explain()`

| Cách viết | Operator trong physical plan | Cơ chế |
|---|---|---|
| Built-in | `Project` | Chạy trong JVM, codegen |
| Python UDF `useArrow=False` | `BatchEvalPython` | Pickle, JVM ↔ Python worker |
| Python UDF `useArrow=True` | `ArrowEvalPython` | Arrow columnar batch |
| Pandas UDF | `ArrowEvalPython` | Arrow + vectorized pandas |

Lưu ý phiên bản: Spark ≤ 3.5 Python UDF mặc định là pickle (`BatchEvalPython`); Spark 4.x mặc định Arrow nên `@udf` thường cũng hiện `ArrowEvalPython`.

### B. Chi phí chuyển dữ liệu JVM ↔ Python (1 triệu dòng, `amount * 1.1`)

| Cách | Thời gian |
|---|---|
| Built-in | 0.22s |
| Python UDF (pickle) | 2.28s |
| Python UDF (`useArrow=True`) | 0.83s |
| Pandas UDF | 0.67s |

Phần lớn chênh lệch đến từ serialize và chuyển dữ liệu giữa JVM và Python, không phải từ phép nhân. Built-in thắng vì không rời JVM. Arrow gửi dữ liệu theo cột, từng batch, nên rẻ hơn pickle từng dòng; Pandas UDF còn vector hóa phép tính.

`spark.sql.execution.arrow.maxRecordsPerBatch` (Pandas UDF, 1 triệu dòng): 100 → 4.14s; 1.000 → 0.93s; 10.000 (mặc định) → 0.66s; 100.000 → 0.63s. Batch quá nhỏ tốn overhead mỗi batch; batch quá lớn tốn RAM của Python worker.

### C. Shuffle

- UDF theo dòng (`withColumn`) **không** shuffle; chỉ có `BatchEvalPython`/`ArrowEvalPython`.
- `groupBy` có `Exchange hashpartitioning`; join mặc định (`SortMergeJoin`) shuffle cả hai bảng.
- `F.broadcast(bảng_nhỏ)` → `BroadcastHashJoin`, bảng lớn không phải shuffle.
- `groupBy().applyInPandas()` = shuffle + Python (`FlatMapGroupsInPandas`), đắt hơn aggregate built-in.
- Số partition sau shuffle = `spark.sql.shuffle.partitions` (đã kiểm chứng 2, 8, 200).
- UDF trong `filter` làm `DataFilters` của `FileScan` rỗng: mất predicate pushdown, Spark đọc hết dữ liệu rồi mới lọc bằng Python. Filter built-in thì được đẩy xuống.

### D. Cache / Persist

Cùng một DataFrame có UDF đắt, gọi 3 action (200.000 dòng):

| | action 1 | action 2 | action 3 |
|---|---|---|---|
| Không cache | 2.64s | 3.60s | 0.88s |
| `persist(MEMORY_AND_DISK)` | 1.35s | 0.38s | 0.52s |

Số đo nhỏ và nhiễu (có warm-up JVM), nhưng xu hướng rõ: sau khi cache, các action sau đọc từ `InMemoryTableScan` và không chạy lại UDF. Chỉ cache khi dùng lại nhiều lần, và nhớ `unpersist()`.

### E. Data skew

Dữ liệu 600.000 dòng, key `HCM` chiếm 80%. Sau `repartition("province")`, một partition có 486.000 dòng, các partition khác 6.000–30.000.

| Cách xử lý | Thời gian |
|---|---|
| `applyInPandas` thẳng theo `province` | 6.70s |
| Salting 2 tầng, built-in | 1.31s (kết quả khớp với không salting) |
| Salting + `applyInPandas` + gộp | 2.99s |
| AQE skew join (join với bảng province) | 3.57s |

Salting chỉ dùng được với phép tính gộp được (sum, count, min, max). AQE chỉ xử lý skew ở **join**, không xử lý skew ở `groupBy`/`applyInPandas`. Ở máy 1 CPU, chênh lệch thời gian không phản ánh đúng cluster thật, nhưng phân bố dòng theo partition thì đúng bản chất skew.

### Câu hỏi ôn thêm

**Vì sao built-in có lợi thế hơn UDF?** Chạy trong JVM, không tốn chi phí chuyển dữ liệu sang Python, được Catalyst tối ưu (pushdown, codegen, gộp biểu thức).

**Khi nào Pandas UDF đáng dùng?** Khi cần pandas/numpy/thư viện ML, hoặc logic không viết được bằng built-in và muốn rẻ hơn Python UDF.

**Hai thứ nên kiểm tra đầu tiên khi job chậm?** Xem `explain()` có `EvalPython` và `Exchange` không; xem Spark UI có task nào chạy lâu hơn hẳn các task khác (dấu hiệu skew).
