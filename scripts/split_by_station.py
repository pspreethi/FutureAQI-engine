import pandas as pd
from pathlib import Path
import os
from logging_util import setup_logger

# === Setup Logging ===
logger = setup_logger("split_by_station", "split_by_station.log")

# === Configuration ===
INPUT_FILE = "data/cleaned_openaq.csv"
OUTPUT_FOLDER = "data/stations"

def split_by_station(input_file=INPUT_FILE, date_col="to_local_date", station_col="name", output_folder=OUTPUT_FOLDER):
    """
    Splits a cleaned, interpolated dataset into one file per station (by 'name'),
    applies daily frequency, and interpolates missing values.
    """
    try:
        # === Load cleaned data ===
        logger.info(f"Loading cleaned data from {input_file}")
        df = pd.read_csv(input_file)

        # === Ensure datetime is parsed ===
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        # === Create output folder ===
        os.makedirs(output_folder, exist_ok=True)

        # === Loop through unique station names ===
        for station in df[station_col].dropna().unique():
            try:
                logger.info(f"Processing station: {station}")

                df_station = df[df[station_col] == station].copy()

                # Set datetime index
                df_station.set_index(date_col, inplace=True)

                # Step 1: Group by date (to remove duplicates)
                df_station = df_station.groupby(df_station.index).mean(numeric_only=True)

                # Step 2: Set daily frequency
                df_station = df_station.asfreq("D")

                # Step 3: Interpolate missing values
                aqi_cols = [
                    'value', 'summary.min', 'summary.q02', 'summary.q25', 'summary.median',
                    'summary.q75', 'summary.q98', 'summary.max', 'summary.avg', 'summary.sd'
                ]
                for col in aqi_cols:
                    if col in df_station.columns:
                        df_station[col] = df_station[col].interpolate(method="time")
                        logger.info(f"Interpolated missing values for {col} in station {station}")

                # Step 4: Save cleaned & interpolated file
                clean_name = station.replace(" ", "_").replace("/", "_")
                output_path = Path(output_folder) / f"{clean_name}.csv"
                df_station.to_csv(output_path)
                logger.info(f"Successfully saved station data to {output_path}")

            except Exception as e:
                logger.error(f"Error processing station {station}: {e}")

        logger.info("All stations saved successfully!")

    except Exception as e:
        logger.error(f"Error in split_by_station: {e}")

# === Run if used standalone ===
if __name__ == "__main__":
    split_by_station()
