from pathlib import Path
import shutil
import time

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType)
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    lit,
    lpad,
    regexp_replace,
    round,
    trim,
    upper,
    sum,
    to_date,
)
import matplotlib.pyplot as plt


class WarehouseRetailSalesAnalysis:
    """
    Distributed Sales Data Analysis using PySpark.
    
    How Spark distributes:
    - Spark uses a master-worker architecture. The driver (master) runs the main()
      and splits the data into partitions (default ~128 MB each).
    - Partitions are distributed across worker nodes in the cluster. Each worker
      processes its partition(s) independently in parallel.
    - For the CSV read, Spark creates a partition for each file split, so
      multiple workers read different chunks simultaneously.
    - Transformations (filter, groupBy, withColumn) are lazy — they build a DAG
      of stages. Actions (show, count, save) trigger job execution: the DAG
      scheduler splits the job into stages, each stage is split into tasks,
      and tasks are sent to workers for parallel execution.
    - Shuffle operations (groupBy, orderBy) repartition data across workers,
      causing network I/O. Spark optimises this with pipelining and codegen.
    - Fault tolerance: if a worker fails, Spark recomputes its partitions from
      lineage (RDD DAG) or from shuffle files on surviving workers.
    """
    COLUMN_MAP = {
        "YEAR": "year",
        "MONTH": "month",
        "SUPPLIER": "supplier",
        "ITEM CODE": "item_code",
        "ITEM DESCRIPTION": "item_description",
        "ITEM TYPE": "item_type",
        "RETAIL SALES": "retail_sales",
        "RETAIL TRANSFERS": "retail_transfers",
        "WAREHOUSE SALES": "warehouse_sales",
    }

    def __init__(self, file_path):
        self.file_path = file_path

        self.spark = SparkSession.builder \
            .appName("WarehouseRetailSalesAnalysis") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "12") \
            .config("spark.default.parallelism", "12") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.schema = StructType([
            StructField("YEAR", IntegerType(), True),
            StructField("MONTH", IntegerType(), True),
            StructField("SUPPLIER", StringType(), True),
            StructField("ITEM CODE", StringType(), True),
            StructField("ITEM DESCRIPTION", StringType(), True),
            StructField("ITEM TYPE", StringType(), True),
            StructField("RETAIL SALES", DoubleType(), True),
            StructField("RETAIL TRANSFERS", DoubleType(), True),
            StructField("WAREHOUSE SALES", DoubleType(), True),
        ])

        self.df = None
        self.cleaned_df = None

    def _output_root(self):
        return Path(self.file_path).resolve().parent.parent / "outputs"

    def load_data(self):
        self.df = self.spark.read.csv(
            self.file_path,
            header=True,
            schema=self.schema,
            quote='"',
            escape='"',
            multiLine=True
        )

        print("=== DATA SAMPLE ===")
        self.df.show(10, truncate=False)

        print("=== SCHEMA ===")
        self.df.printSchema()

    def normalize_data(self):
        normalized = self.df

        for source_name, target_name in self.COLUMN_MAP.items():
            normalized = normalized.withColumnRenamed(source_name, target_name)

        normalized = normalized.select(
            col("year").cast("int").alias("year"),
            col("month").cast("int").alias("month"),
            trim(regexp_replace(col("supplier"), r"\s+", " ")).alias("supplier"),
            trim(col("item_code")).alias("item_code"),
            trim(regexp_replace(col("item_description"), r"\s+", " ")).alias("item_description"),
            upper(trim(regexp_replace(col("item_type"), r"\s+", " "))).alias("item_type"),
            round(col("retail_sales").cast("double"), 2).alias("retail_sales"),
            round(col("retail_transfers").cast("double"), 2).alias("retail_transfers"),
            round(col("warehouse_sales").cast("double"), 2).alias("warehouse_sales"),
        )

        normalized = normalized.withColumn(
            "year_month",
            concat_ws("-", col("year"), lpad(col("month").cast("string"), 2, "0"))
        ).withColumn(
            "sale_date",
            to_date(concat_ws("-", col("year"), lpad(col("month").cast("string"), 2, "0"), lit("01")), "yyyy-MM-dd")
        )

        self.df = normalized

        print("=== NORMALIZED DATA ===")
        self.df.show(10, truncate=False)

    def clean_data(self):
        cleaned = self.df.filter(
            col("year").isNotNull()
            & col("month").isNotNull()
            & col("supplier").isNotNull()
            & (trim(col("supplier")) != "")
            & col("item_code").isNotNull()
            & (trim(col("item_code")) != "")
            & col("item_type").isNotNull()
            & (trim(col("item_type")) != "")
            & col("retail_sales").isNotNull()
            & col("retail_transfers").isNotNull()
            & col("warehouse_sales").isNotNull()
            & col("year").between(1900, 2100)
            & col("month").between(1, 12)
            & (col("retail_sales") >= 0)
            & (col("retail_transfers") >= 0)
            & (col("warehouse_sales") >= 0)
        )

        cleaned = cleaned.withColumn(
            "total_distribution_sales",
            col("retail_sales") + col("retail_transfers") + col("warehouse_sales")
        )

        self.cleaned_df = cleaned

        print("=== CLEANED DATA ===")
        self.cleaned_df.show(10, truncate=False)

        print("=== CLEANING SUMMARY ===")
        print(f"Raw rows: {self.df.count()}")
        print(f"Clean rows: {self.cleaned_df.count()}")

        print("=== TOP CLEANING RULES ===")
        print("- Removed rows with missing critical fields")
        print("- Standardized text casing and whitespace")
        print("- Enforced valid month/year ranges")
        print("- Excluded negative sales values")

    def save_cleaned_data(self, output_path=None):
        if self.cleaned_df is None:
            raise ValueError("Cleaned data is not available. Run clean_data() first.")

        if output_path is None:
            output_path = self._output_root() / "cleaned.csv"
        else:
            output_path = Path(output_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_dir = output_path.parent / f".{output_path.stem}_tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        self.cleaned_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

        part_file = next(
            (path for path in temp_dir.iterdir() if path.is_file() and path.name.startswith("part-")),
            None,
        )
        if part_file is None:
            raise RuntimeError("Failed to locate the exported CSV part file.")

        if output_path.exists():
            output_path.unlink()

        shutil.move(str(part_file), str(output_path))
        shutil.rmtree(temp_dir)

        print(f"Cleaned CSV saved to: {output_path}")

        print("=== WRITING CLEANED DATA TO POSTGRESQL ===")
        
        jdbc_url = "jdbc:postgresql://postgres:5432/tp08"
        
        db_properties = {
            "user": "root",
            "password": "root123",
            "driver": "org.postgresql.Driver"
        }

        try:
            self.cleaned_df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "cleaned_retail_sales") \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .option("driver", db_properties["driver"]) \
                .mode("overwrite") \
                .save()
            print("Successfully transferred all data to PostgreSQL table: 'cleaned_retail_sales'")
        except Exception as e:
            print(f"CRITICAL: Failed to write to PostgreSQL. Error details: {e}")

    def _analysis_frame(self):
        return self.cleaned_df if self.cleaned_df is not None else self.df

    def get_total_retail_sales(self):
        result = self._analysis_frame().agg(
            sum("retail_sales").alias("total_retail_sales")
        )

        print("=== TOTAL RETAIL SALES ===")
        result.show()

        return result

    def get_total_sales(self):
        result = self._analysis_frame().agg(
            sum("retail_sales").alias("total_retail_sales"),
            sum("warehouse_sales").alias("total_warehouse_sales"),
            sum("retail_transfers").alias("total_retail_transfers"),
            (sum("retail_sales") + sum("warehouse_sales") + sum("retail_transfers"))
            .alias("total_distribution_sales"),
        )

        print("=== TOTAL SALES ===")
        result.show()

        return result

    def get_average_retail_sales(self):
        result = self._analysis_frame().agg(
            avg("retail_sales").alias("average_retail_sales")
        )

        print("=== AVERAGE RETAIL SALES ===")
        result.show()

        return result

    def get_sales_by_item_type(self):
        result = (
            self._analysis_frame().groupBy("item_type")
            .agg(
                sum("retail_sales").alias("total_retail_sales"),
                sum("warehouse_sales").alias("total_warehouse_sales"),
            )
            .orderBy(col("total_retail_sales").desc())
        )

        print("=== SALES BY ITEM TYPE ===")
        result.show(truncate=False)

        return result

    def get_top_selling_store(self):
        result = (
            self._analysis_frame().groupBy("item_type")
            .agg(
                (sum("retail_sales") + sum("warehouse_sales") + sum("retail_transfers"))
                .alias("total_distribution_sales")
            )
            .orderBy(col("total_distribution_sales").desc())
        )

        top_store = result.first()

        if top_store:
            print("=== TOP-SELLING STORE ===")
            print(
                f"Store: {top_store['item_type']} "
                f"with total distribution sales "
                f"{top_store['total_distribution_sales']:.2f}"
            )

        return top_store

    def get_top_suppliers(self, limit=10):
        result = (
            self._analysis_frame().groupBy("supplier")
            .agg(
                sum("retail_sales").alias("total_retail_sales")
            )
            .orderBy(col("total_retail_sales").desc())
        )

        print("=== TOP SUPPLIERS ===")
        result.show(limit, truncate=False)

        return result

    def get_top_supplier(self):
        supplier_sales = self.get_top_suppliers()

        top_supplier = supplier_sales.first()

        if top_supplier:
            print(
                f"Top Supplier: "
                f"{top_supplier['supplier']} "
                f"with sales "
                f"{top_supplier['total_retail_sales']:.2f}"
            )

        return top_supplier

    def generate_top_suppliers_chart(
        self,
        output_path=None,
        limit=10
    ):
        supplier_sales = self.get_top_suppliers(limit)

        pandas_df = supplier_sales.limit(limit).toPandas()

        if output_path is None:
            output_path = str(self._output_root() / "top_suppliers_sales.png")

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        plt.figure(figsize=(14, 7))

        plt.bar(
            pandas_df["supplier"],
            pandas_df["total_retail_sales"]
        )

        plt.xticks(rotation=75, ha="right")

        plt.xlabel("Supplier")
        plt.ylabel("Total Retail Sales")
        plt.title(f"Top {limit} Suppliers by Retail Sales")

        plt.tight_layout()

        plt.savefig(output_file)

        print(f"Chart saved to: {output_file}")

    def compare_spark_vs_pandas(self):
        print("\n" + "=" * 60)
        print("SPARK vs PANDAS PERFORMANCE COMPARISON")
        print("=" * 60)

        spark_start = time.time()
        spark_total = self._analysis_frame().agg(
            sum("retail_sales").alias("total_retail_sales")
        ).collect()[0][0]
        spark_end = time.time()
        spark_time = spark_end - spark_start

        pandas_start = time.time()
        pdf = pd.read_csv(
            self.file_path,
            usecols=["RETAIL SALES", "WAREHOUSE SALES", "RETAIL TRANSFERS"]
        )
        pdf = pdf.fillna(0)
        pandas_total = pdf["RETAIL SALES"].sum()
        pandas_end = time.time()
        pandas_time = pandas_end - pandas_start

        print(f"\nTotal Retail Sales (Spark):  {spark_total:.2f}  (took {spark_time:.4f}s)")
        print(f"Total Retail Sales (Pandas): {pandas_total:.2f}  (took {pandas_time:.4f}s)")

        if spark_time < pandas_time:
            ratio = pandas_time / spark_time if spark_time > 0 else float("inf")
            print(f"\nSpark is {ratio:.1f}x faster than Pandas on this machine.")
        else:
            ratio = spark_time / pandas_time if pandas_time > 0 else float("inf")
            print(f"\nPandas is {ratio:.1f}x faster than Spark on this machine (small dataset).")
        print("=" * 60 + "\n")

    def run_analysis(self):
        self.load_data()
        self.normalize_data()
        self.clean_data()
        self.save_cleaned_data()  # Updates both local CSV and Postgres

        self.get_total_sales()
        self.get_total_retail_sales()
        self.get_average_retail_sales()

        self.get_sales_by_item_type()

        self.get_top_selling_store()
        self.get_top_supplier()

        self.generate_top_suppliers_chart()

        self.compare_spark_vs_pandas()

    def stop(self):
        self.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    FILE_PATH = str(Path(__file__).resolve().parents[1] / "database" / "Warehouse_and_Retail_Sales.csv")

    analysis = WarehouseRetailSalesAnalysis(FILE_PATH)

    try:
        analysis.run_analysis()
    finally:
        analysis.stop()