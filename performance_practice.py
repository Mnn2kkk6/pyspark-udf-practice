"""
Phần mở rộng: Performance của UDF + Shuffle / Cache / Data Skew
Yêu cầu: PySpark >= 3.5, pandas, pyarrow
Chạy:    python performance_practice.py              # chạy hết A..E
         python performance_practice.py C D          # chỉ chạy phần C và D
         SCALE=0.2 python performance_practice.py B  # giảm kích thước dữ liệu (máy yếu)
Mỗi phần (A..E) độc lập, đọc output từ trên xuống.
"""
import os
import sys
import time

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder.appName("udf-performance")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")  # tắt AQE để thấy plan/skew "thô"; phần E bật lại
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # phần C tự bật broadcast bằng hint
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


SECTIONS = set(a.upper() for a in sys.argv[1:]) or set("ABCDE")
SCALE = float(os.environ.get("SCALE", "1"))


def n(x):
    """Kích thước dữ liệu sau khi nhân SCALE."""
    return max(1000, int(x * SCALE))


def want(section):
    return section in SECTIONS


def title(text):
    print(f"\n{'=' * 70}\n{text}\n{'=' * 70}")


def timed(label, fn):
    """Chạy fn() một lần (đã warm-up trước đó), in thời gian."""
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"  {label:<38}: {elapsed:6.2f}s")
    return elapsed, result


# Cùng một phép tính (amount * 1.1) viết 4 kiểu
@udf("double", useArrow=False)  # ép dùng pickle (cơ chế cổ điển) -> BatchEvalPython
def py_udf(x):
    return None if x is None else x * 1.1


@udf("double", useArrow=True)  # Python UDF truyền bằng Arrow (Spark >= 3.5; mặc định bật từ Spark 4.0)
def py_udf_arrow(x):
    return None if x is None else x * 1.1


@pandas_udf("double")
def pd_udf(s: pd.Series) -> pd.Series:
    return s * 1.1


def run_sum(df, col):
    return df.agg(F.sum(col)).collect()  # action ép Spark thực thi


if want("A"):
    # ===================================================================
    # A. explain(): nhận biết UDF trong physical plan
    # ===================================================================
    title("A. explain() - BatchEvalPython / ArrowEvalPython / Project")
    base = spark.range(10).withColumn("amount", F.col("id").cast("double") * 1000)

    print("--- Built-in ---")
    base.withColumn("r", F.col("amount") * 1.1).explain()
    print("--- Python UDF useArrow=False (pickle) ---")
    base.withColumn("r", py_udf("amount")).explain()
    print("--- Python UDF + useArrow=True ---")
    base.withColumn("r", py_udf_arrow("amount")).explain()
    print("--- Pandas UDF ---")
    base.withColumn("r", pd_udf("amount")).explain()
    print(
        "Cách đọc:\n"
        "  * Chỉ có 'Project'            -> chạy hoàn toàn trong JVM (codegen)\n"
        "  * 'BatchEvalPython'           -> Python UDF kiểu pickle (useArrow=False; mặc định ở Spark <= 3.5)\n"
        "  * 'ArrowEvalPython'           -> Pandas UDF, hoặc Python UDF useArrow=True (mặc định ở Spark 4.x)\n"
        "  Ngoài ra UDF luôn là 'hộp đen': Catalyst không đẩy filter xuống, không gộp biểu thức."
    )


if want("B"):
    # ===================================================================
    # B. Chi phí JVM <-> Python và Arrow batch
    # ===================================================================
    title("B. Benchmark: chi phí chuyển dữ liệu JVM <-> Python")
    for size in (n(1_000_000), n(5_000_000)):
        df = spark.range(size).withColumn("amount", (F.col("id") * 7 % 50_000_000).cast("double"))
        run_sum(df.withColumn("r", F.col("amount") * 1.1), "r")  # warm-up JVM
        print(f"\nN = {size:,}")
        t_b, _ = timed("Built-in", lambda: run_sum(df.withColumn("r", F.col("amount") * 1.1), "r"))
        t_p, _ = timed("Python UDF (pickle)", lambda: run_sum(df.withColumn("r", py_udf("amount")), "r"))
        t_a, _ = timed("Python UDF (useArrow=True)", lambda: run_sum(df.withColumn("r", py_udf_arrow("amount")), "r"))
        t_d, _ = timed("Pandas UDF (Arrow, vectorized)", lambda: run_sum(df.withColumn("r", pd_udf("amount")), "r"))
        print(f"  => Python UDF chậm hơn built-in ~{t_p / t_b:.0f}x, Pandas UDF ~{t_d / t_b:.0f}x")

    print("\n--- Ảnh hưởng của spark.sql.execution.arrow.maxRecordsPerBatch (Pandas UDF, 5M dòng ở SCALE=1) ---")
    df = spark.range(n(5_000_000)).withColumn("amount", (F.col("id") * 7 % 50_000_000).cast("double"))
    for batch in (100, 1_000, 10_000, 100_000):
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
        timed(f"maxRecordsPerBatch = {batch:,}", lambda: run_sum(df.withColumn("r", pd_udf("amount")), "r"))
    spark.conf.unset("spark.sql.execution.arrow.maxRecordsPerBatch")
    print(
        "Batch quá nhỏ -> overhead mỗi batch (gọi hàm Python, tạo Arrow record batch) lớn.\n"
        "Batch quá lớn -> tốn RAM của Python worker. Mặc định 10.000 là điểm cân bằng thường dùng."
    )


if want("C"):
    # ===================================================================
    # C. Shuffle
    # ===================================================================
    title("C. Shuffle - Exchange trong plan")
    orders = (
        spark.range(n(2_000_000))
        .withColumn("customer_id", (F.col("id") % 1000).cast("int"))
        .withColumn("amount", (F.col("id") % 997).cast("double"))
    )
    dim = spark.range(1000).select(F.col("id").cast("int").alias("customer_id"),
                                   F.concat(F.lit("KH"), F.col("id")).alias("name"))

    print("--- 1) UDF row-level (select/withColumn): KHÔNG shuffle ---")
    orders.withColumn("r", py_udf("amount")).explain()

    print("--- 2) groupBy: có Exchange hashpartitioning (shuffle) ---")
    orders.groupBy("customer_id").agg(F.sum("amount")).explain()

    print("--- 3) Join mặc định (sort-merge): shuffle CẢ HAI bảng ---")
    orders.join(dim, "customer_id").explain()

    print("--- 4) Broadcast join: bảng nhỏ gửi tới mọi executor, bảng lớn KHÔNG shuffle ---")
    orders.join(F.broadcast(dim), "customer_id").explain()

    print("--- 5) Pandas UDF dạng group (applyInPandas) = shuffle + Python ---")


    def top_amount(pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf.nlargest(1, "amount")[["customer_id", "amount"]]


    orders.groupBy("customer_id").applyInPandas(top_amount, "customer_id int, amount double").explain()

    print("--- 6) Số partition sau shuffle = spark.sql.shuffle.partitions ---")
    for p in (2, 8, 200):
        spark.conf.set("spark.sql.shuffle.partitions", str(p))
        n_part = orders.groupBy("customer_id").count().rdd.getNumPartitions()
        print(f"  shuffle.partitions={p:<4} -> {n_part} partition ở stage sau")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    print("--- 7) UDF trong filter chặn predicate pushdown (đọc Parquet) ---")
    orders.write.mode("overwrite").parquet("output/orders_demo")
    pq = spark.read.parquet("output/orders_demo")
    print("Built-in filter:")
    pq.filter(F.col("amount") > 900).explain()
    print("UDF filter:")


    @udf("boolean")
    def gt900(x):
        return x is not None and x > 900


    pq.filter(gt900("amount")).explain()
    print("Tìm 'PushedFilters' / 'DataFilters' trong FileScan: built-in có điều kiện amount > 900.0, UDF thì rỗng []"
          " -> Spark đọc toàn bộ file rồi mới đẩy sang Python lọc.")


if want("D"):
    # ===================================================================
    # D. Cache / Persist
    # ===================================================================
    title("D. Cache / Persist - tránh chạy lại UDF đắt tiền")
    heavy = (
        spark.range(n(1_000_000))
        .withColumn("amount", (F.col("id") * 7 % 50_000_000).cast("double"))
        .withColumn("r", py_udf("amount"))  # bước đắt
    )

    print("--- Không cache: mỗi action chạy lại toàn bộ UDF ---")
    timed("action 1 (count)", lambda: heavy.count())
    timed("action 2 (sum)", lambda: heavy.agg(F.sum("r")).collect())
    timed("action 3 (max)", lambda: heavy.agg(F.max("r")).collect())

    print("--- Có cache (MEMORY_AND_DISK): UDF chỉ chạy ở action đầu ---")
    cached = heavy.persist(StorageLevel.MEMORY_AND_DISK)
    timed("action 1 (count, tính + ghi cache)", lambda: cached.count())
    timed("action 2 (sum, đọc cache)", lambda: cached.agg(F.sum("r")).collect())
    timed("action 3 (max, đọc cache)", lambda: cached.agg(F.max("r")).collect())
    print("--- Plan khi đã cache: đọc từ InMemoryTableScan; BatchEvalPython chỉ nằm trong InMemoryRelation (đã tính xong, không chạy lại) ---")
    cached.agg(F.sum("r")).explain()
    cached.unpersist()

    print(
        "\nLưu ý:\n"
        "  * cache() = persist(MEMORY_AND_DISK) với DataFrame; là lazy, phải có action mới thực sự cache.\n"
        "  * Chỉ cache khi DataFrame được dùng LẠI nhiều lần; cache thừa chỉ tốn RAM.\n"
        "  * Luôn unpersist() khi xong. Xem tab Storage trong Spark UI (http://localhost:4040)."
    )


if want("E"):
    # ===================================================================
    # E. Data skew
    # ===================================================================
    title("E. Data skew - một key chiếm phần lớn dữ liệu")
    N = n(3_000_000)
    skewed = (
        spark.range(N)
        .withColumn(
            "province",
            F.when(F.col("id") % 100 < 80, F.lit("HCM"))  # 80% dòng cùng 1 key
            .otherwise(F.concat(F.lit("P"), (F.col("id") % 20).cast("string"))),
        )
        .withColumn("amount", (F.col("id") % 1000).cast("double"))
    )

    print("--- Phân bố dòng theo key ---")
    skewed.groupBy("province").count().orderBy(F.desc("count")).show(5)

    print("--- Số dòng mỗi partition SAU shuffle theo key (partition của 'HCM' phình to) ---")
    (
        skewed.repartition(8, "province")
        .groupBy(F.spark_partition_id().alias("pid"))
        .count()
        .orderBy("pid")
        .show()
    )

    # Pandas UDF theo nhóm: logic cần Python nên không thay bằng built-in được
    def summarize(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"province": [pdf["province"].iloc[0]],
                             "total": [pdf["amount"].sum()],
                             "n": [len(pdf)]})


    print("--- Không xử lý skew: applyInPandas gom cả 80% dữ liệu của 'HCM' vào 1 task ---")
    t_skew, _ = timed(
        "applyInPandas theo province",
        lambda: skewed.groupBy("province").applyInPandas(summarize, "province string, total double, n long").collect(),
    )

    print("--- Cách 1: Salting + 2 tầng (aggregate từng phần rồi gộp). Dùng built-in ---")
    SALT = 8
    t_salt, salted = timed(
        "salting 2 tầng",
        lambda: (
            skewed.withColumn("salt", (F.rand(seed=42) * SALT).cast("int"))
            .groupBy("province", "salt")
            .agg(F.sum("amount").alias("partial_total"), F.count("*").alias("partial_n"))
            .groupBy("province")
            .agg(F.sum("partial_total").alias("total"), F.sum("partial_n").alias("n"))
            .collect()
        ),
    )
    print("  Kiểm tra salting không đổi kết quả:",
          sorted((r.province, r.total, r.n) for r in salted)
          == sorted((r.province, r.total, r.n) for r in
                    skewed.groupBy("province").agg(F.sum("amount").alias("total"), F.count("*").alias("n")).collect()))

    print("--- Cách 2: Salting cho cả logic Python (Pandas UDF chạy trên từng mảnh nhỏ) ---")


    def partial(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"province": [pdf["province"].iloc[0]],
                             "partial_total": [pdf["amount"].sum()],
                             "partial_n": [len(pdf)]})


    timed(
        "salt + applyInPandas + gộp",
        lambda: (
            skewed.withColumn("salt", (F.rand(seed=42) * SALT).cast("int"))
            .groupBy("province", "salt")
            .applyInPandas(partial, "province string, partial_total double, partial_n long")
            .groupBy("province")
            .agg(F.sum("partial_total").alias("total"), F.sum("partial_n").alias("n"))
            .collect()
        ),
    )
    print("  Chỉ dùng được khi phép tính 'gộp được' (sum/count/min/max). Median, distinct count chính xác thì không.")

    print("--- Cách 3: AQE tự xử lý skew join (Spark >= 3.0) ---")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    province_dim = spark.createDataFrame([("HCM", "South")] + [(f"P{i}", "Other") for i in range(20)],
                                         "province string, region string")
    joined = skewed.join(province_dim, "province")
    timed("join skewed với AQE bật", lambda: joined.groupBy("region").count().collect())
    print("  AQE tách partition quá lớn thành nhiều phần nhỏ khi join (xem 'AQEShuffleRead ... skewed' trong plan).")
    print("  AQE chỉ lo skew ở JOIN; skew ở groupBy/applyInPandas vẫn phải salting thủ công.")


spark.stop()
