import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset
from evidently.test_suite import TestSuite
from evidently.tests import TestNumberOfColumnsWithMissingValues, TestShareOfMissingValues
from pathlib import Path
from utils.logger import get_logger

logger = get_logger("Monitoring")

BASE_DIR = Path(__file__).resolve().parent.parent 
# Config
INPUT_FILE = BASE_DIR / "datasets/training_data.csv"
REPORT_OUTPUT = BASE_DIR / "monitoring/drift_report.html"

def generate_drift_report():
    logger.info(" [~] Analyzing Data Drift.")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        logger.error(" [x] Data not found.")
        return

    # 2. Split Reference (Past) vs Current (Recent)
    # In a real scenario, Reference = Training Data, Current = Production Data.
    # Here, we split our existing history to check for "Internal Drift".
    # If the first half of the month is different from the second half, our model might struggle.
    mid_point = int(len(df) * 0.5)
    
    reference_data = df.iloc[:mid_point]
    current_data = df.iloc[mid_point:]
    
    # 3. Define the Report
    # We use Presets to get a lot of metrics for free
    drift_report = Report(metrics=[
        DataDriftPreset(),      # Did the distribution of features change? (KS-Test)
        DataQualityPreset(),    # Are there more NaNs than before?
        TargetDriftPreset()     # Did the target (Stock Out) frequency change?
    ])
    
    logger.info(" [~] Calculating metrics...")
    drift_report.run(reference_data=reference_data, current_data=current_data)
    
    # 4. Save HTML
    drift_report.save_html(str(REPORT_OUTPUT)) #? Because Path object doesn't have any write attribute, and save_html uses write internally 
    logger.info(f" [OK] Report saved to {REPORT_OUTPUT}")
    logger.info(f" [OK] Open this {REPORT_OUTPUT} file in the browser to inspect the data.")

if __name__ == "__main__":
    generate_drift_report()