"""
Bài thực hành: UDF / Pandas UDF / UDTF trong PySpark
Yêu cầu: PySpark >= 3.5 (UDTF Python), pandas, pyarrow
Chạy:    python udf_practice.py
Output:  output/customers_processed, output/customer_tags (Parquet)
"""
import re
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf, udtf
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder.appName("udf-pandas-udf-udtf")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 0. Đọc dữ liệu
# ---------------------------------------------------------------
customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/customers.csv")
)
print("=== customers (raw) ===")
customers.show(truncate=False)


# ---------------------------------------------------------------
# YÊU CẦU 1 - Python UDF
# ---------------------------------------------------------------
@udf(returnType=StringType())
def clean_name_udf(name):
    """trim + gộp nhiều khoảng trắng + format Title Case."""
    if name is None:
        return None
    return re.sub(r"\s+", " ", name.strip()).title()


@udf(returnType=StringType())
def segment_udf(amount):
    """VIP >= 10 triệu, STANDARD >= 3 triệu, còn lại BASIC."""
    if amount is None:
        return "BASIC"
    if amount >= 10_000_000:
        return "VIP"
    if amount >= 3_000_000:
        return "STANDARD"
    return "BASIC"


df_udf = (
    customers.withColumn("customer_name_clean", clean_name_udf("customer_name"))
    .withColumn("customer_segment", segment_udf("amount"))
)
print("=== Yêu cầu 1: Python UDF ===")
df_udf.select("customer_id", "customer_name_clean", "amount", "customer_segment").show(truncate=False)

# ---------------------------------------------------------------
# YÊU CẦU 2 - Built-in Spark function (cùng logic)
# ---------------------------------------------------------------
df_builtin = (
    customers.withColumn(
        "customer_name_clean",
        F.initcap(F.regexp_replace(F.trim(F.col("customer_name")), r"\s+", " ")),
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("amount") >= 10_000_000, "VIP")
        .when(F.col("amount") >= 3_000_000, "STANDARD")
        .otherwise("BASIC"),
    )
)
print("=== Yêu cầu 2: Built-in ===")
df_builtin.select("customer_id", "customer_name_clean", "amount", "customer_segment").show(truncate=False)

# Kiểm tra 2 cách cho kết quả giống nhau
cols = ["customer_id", "customer_name_clean", "customer_segment"]
diff = df_udf.select(*cols).exceptAll(df_builtin.select(*cols)).count()
print(f"Số dòng khác nhau giữa UDF và built-in: {diff}")

# Xem physical plan: UDF sinh BatchEvalPython, built-in thì không
print("--- Plan UDF ---")
df_udf.explain()
print("--- Plan Built-in ---")
df_builtin.explain()

# ---------------------------------------------------------------
# YÊU CẦU 3 - Pandas UDF
# ---------------------------------------------------------------
@udf("double")
def add_rate_py_udf(amount):
    return None if amount is None else round(amount * 1.1, 2)


@pandas_udf("double")
def add_rate_pandas_udf(amount: pd.Series) -> pd.Series:
    """Cộng thêm 10% - xử lý cả batch (vectorized) bằng pandas/numpy."""
    return (amount * 1.1).round(2)


df_pandas = (
    df_udf.withColumn("amount_with_rate", add_rate_pandas_udf(F.col("amount").cast("double")))
)
print("=== Yêu cầu 3: Pandas UDF ===")
df_pandas.select("customer_id", "amount", "amount_with_rate").show()

# Benchmark nhỏ: Python UDF vs Pandas UDF trên 1 triệu dòng
big = spark.range(1_000_000).withColumn("amount", (F.col("id") * 7 % 50_000_000).cast("double"))


def timeit(label, df):
    start = time.time()
    df.agg(F.sum("r")).collect()  # ép Spark thực thi
    print(f"{label:<12}: {time.time() - start:.2f}s")


timeit("Python UDF", big.withColumn("r", add_rate_py_udf("amount")))
timeit("Pandas UDF", big.withColumn("r", add_rate_pandas_udf("amount")))
timeit("Built-in", big.withColumn("r", F.col("amount") * 1.1))

# ---------------------------------------------------------------
# YÊU CẦU 4 - UDTF (1 dòng -> nhiều dòng)
# ---------------------------------------------------------------
@udtf(returnType="customer_id: int, tag: string")
class SplitTags:
    def eval(self, customer_id: int, tags: str):
        if tags is None:
            return
        for tag in tags.split(","):
            tag = tag.strip()
            if tag:
                yield customer_id, tag


tags_df = spark.createDataFrame(
    [(1, "spark,python,etl"), (2, "sql,airflow"), (3, "python,pandas,ml,etl")],
    "customer_id int, tags string",
)
print("=== Yêu cầu 4: UDTF ===")
tags_df.show(truncate=False)

# Cách 1: gọi UDTF qua SQL với LATERAL join
spark.udtf.register("split_tags", SplitTags)
tags_df.createOrReplaceTempView("customer_tags_raw")
customer_tags = spark.sql(
    """
    SELECT t.customer_id, s.tag
    FROM customer_tags_raw t,
         LATERAL split_tags(t.customer_id, t.tags) s
    """
)
customer_tags.show()

# Cách 2 (built-in, ưu tiên khi đủ dùng): split + explode
customer_tags_builtin = tags_df.select(
    "customer_id", F.explode(F.split("tags", ",")).alias("tag")
)
print("Built-in explode cho kết quả giống UDTF:",
      customer_tags.exceptAll(customer_tags_builtin).count() == 0)

# ---------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------
customers_processed = df_pandas.select(
    "customer_id", "customer_name", "customer_name_clean", "province",
    "amount", "amount_with_rate", "customer_segment", "note",
)
customers_processed.write.mode("overwrite").parquet("output/customers_processed")
customer_tags.write.mode("overwrite").parquet("output/customer_tags")

print("=== customers_processed ===")
spark.read.parquet("output/customers_processed").show(truncate=False)
print("=== customer_tags ===")
spark.read.parquet("output/customer_tags").orderBy("customer_id", "tag").show()

spark.stop()
