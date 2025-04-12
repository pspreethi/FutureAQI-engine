import subprocess
import logging
from datetime import datetime

# === Setup Logging ===
LOG_FILE = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def run_script(script_name, description):
    try:
        logging.info(f"Starting: {description}")
        print(f"Running: {description}...")
        result = subprocess.run(["python3", script_name], check=True, capture_output=True, text=True)
        logging.info(f"Completed: {description}")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {description}: {e.stderr}")
        print(f"Error running {script_name}: {e.stderr}")

if __name__ == "__main__":
    try:
        logging.info("Pipeline execution started.")

        # Step 1: Fetch data
        run_script("scripts/fetch_data.py", "Data Fetching")

        # Step 2: Clean data
        run_script("scripts/clean_data.py", "Data Cleaning")

        # Step 3: Split data into stations
        run_script("scripts/split_by_station.py", "Data Segregation by Station")

        # Step 4: Generate EDA
        run_script("scripts/station_eda.py", "EDA Generation")

        # Step 5: Forecast AQI
        run_script("scripts/forecast_data.py", "AQI Forecasting")

        # Step 6: Evaluate forecast
        run_script("scripts/evaluate_forecast.py", "Forecast Evaluation")

        logging.info("Pipeline execution completed successfully.")
        print("Pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        print(f"Pipeline execution failed: {e}")
