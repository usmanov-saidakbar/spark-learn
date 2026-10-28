"""Starter kit for the 2-week Spark roadmap (PySpark 4.2 + Delta Lake 4.4).

Save this file next to your notebooks or scripts, then:

    from lab_utils import *
    spark = get_spark()

Everything here is plain PySpark, so read it once on Day 1. It exists so you
spend lab time on plans and the Spark UI, not on boilerplate.
"""
import contextlib
import io
import os
import re
import sys
import time

# Executors start their own Python worker processes. Point them at this
# interpreter, otherwise they run whatever `python3` is first on PATH. On macOS
# that is Python 3.9, which PySpark 4.2 cannot import, and every UDF fails.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402
from pyspark.sql.functions import pandas_udf, udf  # noqa: E402

SPARK_VERSION = "4.2.0"
AVRO_PACKAGE = f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION}"
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}
# Spark 4.2.0 feeds Parquet's columnar batches straight into Arrow Python UDFs.
# That path is broken (a plain @udf on a Parquet read can hang or return wrong
# results, SPARK-60294) and is off by default from 4.2.1, so switch it off here.
DEFAULT_CONF = {"spark.sql.execution.arrow.pythonUDF.columnarInput.enabled": "false"}


def get_spark(app="spark-roadmap", cores=4, driver_memory="2g", delta=True, avro=True,
              master=None, conf=None):
    """Start a local session, or return the one already running.

    Settings that size the JVM (driver_memory) only apply in a fresh Python
    process, before the first session starts. Pass other settings as a dict:
    get_spark(conf={"spark.sql.shuffle.partitions": "8"}).
    master overrides local[cores], e.g. "local[4,3]" allows 3 attempts per task.
    The first Delta/Avro start downloads jars, which takes a minute or two.
    """
    builder = (
        SparkSession.builder.master(master or f"local[{cores}]")
        .appName(app)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in {**DEFAULT_CONF, **(DELTA_CONF if delta else {}), **(conf or {})}.items():
        builder = builder.config(key, value)
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, extra_packages=[AVRO_PACKAGE] if avro else None)
    elif avro:
        builder = builder.config("spark.jars.packages", AVRO_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} running, UI at {spark.sparkContext.uiWebUrl}")
    return spark


# --- Reading plans -------------------------------------------------------------

def plan_text(df: DataFrame, mode="formatted") -> str:
    """explain() output as a string, so you can save it to your notes or diff it."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode)
    return buf.getvalue()


def count_nodes(df: DataFrame, *names, mode="simple") -> dict:
    """Count plan nodes, e.g. count_nodes(df, "Exchange", "Sort", "BroadcastHashJoin")."""
    text = plan_text(df, mode)
    return {name: len(re.findall(r"\b" + re.escape(name) + r"\b", text)) for name in names}


# --- Running and timing --------------------------------------------------------

def run(df: DataFrame, label="query") -> float:
    """Execute the whole plan without collecting rows (noop sink) and print the time.

    The noop write is a new query, so read its plan in the SQL / DataFrame tab.
    To see the AQE final plan with explain(), call df.collect() on a small result.
    """
    spark = df.sparkSession
    spark.sparkContext.setJobDescription(label)
    start = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    seconds = time.perf_counter() - start
    spark.sparkContext.setJobDescription(None)
    print(f"{label}: {seconds:.2f}s")
    return seconds


def partition_sizes(df: DataFrame) -> list:
    """Rows per partition, largest first. One huge number means skew."""
    rows = df.groupBy(F.spark_partition_id().alias("p")).count().collect()
    return sorted((r["count"] for r in rows), reverse=True)


# --- Synthetic data (seeded, so every run gives the same rows) ------------------

def make_customers(spark, n=10_000):
    """customers(customer_id, name, country, segment); 20 countries, 4 segments."""
    return spark.range(n).select(
        F.col("id").alias("customer_id"),
        F.concat(F.lit("customer_"), F.col("id").cast("string")).alias("name"),
        F.concat(F.lit("C"), (F.col("id") % 20).cast("string")).alias("country"),
        F.element_at(F.array(*map(F.lit, ["retail", "smb", "enterprise", "public"])),
                     (F.col("id") % 4 + 1).cast("int")).alias("segment"),
    )


def make_products(spark, n=1_000):
    """products(product_id, category, price); 20 categories."""
    return spark.range(n).select(
        F.col("id").alias("product_id"),
        F.concat(F.lit("cat_"), (F.col("id") % 20).cast("string")).alias("category"),
        F.round(F.rand(11) * 200 + 1, 2).alias("price"),
    )


def make_orders(spark, n=2_000_000, customers=10_000, products=1_000, days=30,
                hot_share=0.0, null_share=0.0, seed=7):
    """orders(order_id, customer_id, product_id, amount, status, order_ts, order_date, note).

    hot_share:  fraction of rows whose customer_id is 0 (one hot key, for skew labs).
    null_share: fraction of rows whose customer_id is null.
    note is a 32-character hash, so rows carry real bytes through a shuffle
    (AQE measures skew in shuffle bytes, not rows).
    """
    # Materialize the draw once: two rand() calls inside one CASE WHEN drift apart,
    # because the second is only evaluated when the first branch is false.
    r = F.col("_r")
    customer = (
        F.when(r < null_share, F.lit(None).cast("long"))
        .when(r < null_share + hot_share, F.lit(0).cast("long"))
        .otherwise((F.rand(seed + 1) * (customers - 1) + 1).cast("long"))
    )
    start = F.lit("2026-01-01 00:00:00").cast("timestamp")
    ts = start + F.make_interval(secs=(F.rand(seed + 2) * days * 86_400).cast("int"))
    return spark.range(n).withColumn("_r", F.rand(seed)).select(
        F.col("id").alias("order_id"),
        customer.alias("customer_id"),
        (F.rand(seed + 3) * products).cast("long").alias("product_id"),
        F.round(F.rand(seed + 4) * 100, 2).alias("amount"),
        F.element_at(F.array(*map(F.lit, ["new", "paid", "shipped", "cancelled"])),
                     (F.rand(seed + 5) * 4 + 1).cast("int")).alias("status"),
        ts.alias("order_ts"),
        F.to_date(ts).alias("order_date"),
        F.md5(F.col("id").cast("string")).alias("note"),
    )


__all__ = [
    "F", "StorageLevel", "udf", "pandas_udf", "get_spark", "plan_text", "count_nodes", "run",
    "partition_sizes", "make_customers", "make_products", "make_orders",
]
