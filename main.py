from src.app.sale_analysis import WarehouseRetailSalesAnalysis
from pathlib import Path


def main():
    file_path = Path(__file__).resolve().parent / "src" / "database" / "Warehouse_and_Retail_Sales.csv"
    analysis = WarehouseRetailSalesAnalysis(file_path)

    try:
        analysis.run_analysis()
    finally:
        analysis.stop()


if __name__ == "__main__":
    main()
