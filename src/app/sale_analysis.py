from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType)
from pyspark.sql.functions import sum, avg, col
import matplotlib.pyplot as plt


class WarehouseRetailSalesAnalysis:

    def __init__(self, file_path):
        self.file_path = file_path

        self.spark = SparkSession.builder \
            .appName("WarehouseRetailSalesAnalysis") \
            .master("spark://spark-master:7077") \
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
            StructField("WAREHOUSE SALES", DoubleType(), True)
        ])

        self.df = None

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

    def clean_data(self):
        self.df = self.df.dropna(subset=[
            "RETAIL SALES",
            "WAREHOUSE SALES"
        ])

        print("=== CLEANED DATA ===")
        self.df.show(10, truncate=False)

    def get_total_retail_sales(self):
        result = self.df.agg(
            sum("RETAIL SALES").alias("total_retail_sales")
        )

        print("=== TOTAL RETAIL SALES ===")
        result.show()

        return result

    def get_average_retail_sales(self):
        result = self.df.agg(
            avg("RETAIL SALES").alias("average_retail_sales")
        )

        print("=== AVERAGE RETAIL SALES ===")
        result.show()

        return result

    def get_sales_by_item_type(self):
        result = (
            self.df.groupBy("ITEM TYPE")
            .agg(
                sum("RETAIL SALES").alias("total_retail_sales"),
                sum("WAREHOUSE SALES").alias("total_warehouse_sales")
            )
            .orderBy(col("total_retail_sales").desc())
        )

        print("=== SALES BY ITEM TYPE ===")
        result.show(truncate=False)

        return result

    def get_top_suppliers(self, limit=10):
        result = (
            self.df.groupBy("SUPPLIER")
            .agg(
                sum("RETAIL SALES").alias("total_retail_sales")
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
                f"{top_supplier['SUPPLIER']} "
                f"with sales "
                f"{top_supplier['total_retail_sales']:.2f}"
            )

        return top_supplier

    def generate_top_suppliers_chart(
        self,
        output_path="/opt/spark-apps/top_suppliers_sales.png",
        limit=10
    ):
        supplier_sales = self.get_top_suppliers(limit)

        pandas_df = supplier_sales.limit(limit).toPandas()

        plt.figure(figsize=(14, 7))

        plt.bar(
            pandas_df["SUPPLIER"],
            pandas_df["total_retail_sales"]
        )

        plt.xticks(rotation=75, ha="right")

        plt.xlabel("Supplier")
        plt.ylabel("Total Retail Sales")
        plt.title(f"Top {limit} Suppliers by Retail Sales")

        plt.tight_layout()

        plt.savefig(output_path)

        print(f"Chart saved to: {output_path}")

    def run_analysis(self):
        self.load_data()
        self.clean_data()

        self.get_total_retail_sales()
        self.get_average_retail_sales()

        self.get_sales_by_item_type()

        self.get_top_supplier()

        self.generate_top_suppliers_chart()

    def stop(self):
        self.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":

    FILE_PATH = "/opt/spark-apps/database/Warehouse_and_Retail_Sales.csv"

    analysis = WarehouseRetailSalesAnalysis(FILE_PATH)

    try:
        analysis.run_analysis()

    finally:
        analysis.stop()