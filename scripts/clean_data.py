import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from logging_util import setup_logger 

# === Setup ===
load_dotenv()

# Initialize logger
logger = setup_logger("clean_data", "clean_data.log")

# === File paths ===
RAW_DATA_PATH = "data/openaq_combined_data.csv"
LOCATIONS_PATH = "data/locations.csv"
OUTPUT_RAW_PATH = "data/cleaned_openaq_not_interpolated.csv"
OUTPUT_PATH = "data/cleaned_openaq.csv"

# === Datetime parsing helper ===
def parse_coverage_datetimes(df):
    logger.info("Parsing coverage datetime columns...")
    datetime_cols = [
        "coverage.datetimeFrom.utc",
        "coverage.datetimeFrom.local",
        "coverage.datetimeTo.utc",
        "coverage.datetimeTo.local"
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            logger.info(f"Converted {col} to datetime format")

    # Create new date-only columns
    if "coverage.datetimeFrom.utc" in df.columns:
        df["from_utc_date"] = df["coverage.datetimeFrom.utc"].dt.date
    if "coverage.datetimeFrom.local" in df.columns:
        df["from_local_date"] = pd.to_datetime(df["coverage.datetimeFrom.local"], errors="coerce").dt.date
    if "coverage.datetimeTo.utc" in df.columns:
        df["to_utc_date"] = df["coverage.datetimeTo.utc"].dt.date
    if "coverage.datetimeTo.local" in df.columns:
        df["to_local_date"] = pd.to_datetime(df["coverage.datetimeTo.local"], errors="coerce").dt.date

    logger.info("Datetime parsing completed.")
    return df

# === Interpolate data ===
def interpolate_openaq_data(df):
    logger.info("Starting data interpolation...")
    aqi_columns = [
        'value', 'summary.min', 'summary.q02', 'summary.q25',
        'summary.median', 'summary.q75', 'summary.q98',
        'summary.max', 'summary.avg', 'summary.sd'
    ]

    logger.info(f"Data before cleaning: {df.describe()}")

    # Setting datetime column as index
    df["from_local_date"] = pd.to_datetime(df["from_local_date"])
    df.set_index("from_local_date", inplace=True)

    # Replace negative values with NaN
    for col in aqi_columns:
        neg_count = (df[col] < 0).sum()
        df[col] = df[col].where(df[col] >= 0, np.nan)
        logger.info(f"{col}: {neg_count} negative values replaced with NaN")

    # Interpolate missing values
    df[aqi_columns] = df[aqi_columns].interpolate(method='time')
    logger.info("Interpolation completed.")
    return df

# === Clean data ===
def clean_openaq_data():
    try:
        logger.info("Loading raw data...")
        df_raw = pd.read_csv(RAW_DATA_PATH)
        df_locations = pd.read_csv(LOCATIONS_PATH)

        logger.info("Merging data with location information...")
        df = pd.merge(df_raw, df_locations, left_on='sensor_id', right_on='s_id', how='left')

        logger.info("Starting datetime parsing...")
        df = parse_coverage_datetimes(df)

        # Combine parameter and units
        df["parameter"] = df["parameter.name"].astype(str) + " " + df["parameter.units"].astype(str)

        # Drop unnecessary columns
        columns_to_drop = [
            'coordinates', 'flagInfo.hasFlags', 'parameter.id', 
            'parameter.name', 'parameter.units', 'parameter.displayName',
            'period.label', 'period.interval', 'period.datetimeFrom.utc', 'period.datetimeFrom.local',
            'period.datetimeTo.utc', 'period.datetimeTo.local',
            'coverage.expectedCount', 'coverage.expectedInterval',
            'coverage.observedCount', 'coverage.observedInterval',
            'coverage.percentComplete', 'coverage.percentCoverage',
            'coverage.datetimeFrom.utc', 'coverage.datetimeFrom.local',
            'coverage.datetimeTo.utc', 'coverage.datetimeTo.local',
            'datetimeFirst.utc', 'datetimeFirst.local', 'datetimeLast.utc', 'datetimeLast.local',
            'distance', 's_id', 's_parameter.id', 's_parameter.name', 
            's_parameter.units', 's_parameter.displayName'
        ]
        df = df.drop(columns=columns_to_drop, errors='ignore')
        logger.info("Dropped unnecessary columns.")

        # Final columns for analysis
        final_cols = [
            'value', 'sensor_id', 'summary.min', 'summary.q02', 'summary.q25',
            'summary.median', 'summary.q75', 'summary.q98', 'summary.max',
            'summary.avg', 'summary.sd', 'from_utc_date', 'from_local_date',
            'to_utc_date', 'to_local_date', 'parameter', 'provider.id', 
            'provider.name', 'id', 'name', 'locality'
        ]
        df_analysis = df[final_cols]

        logger.info(f"Saving cleaned data to {OUTPUT_RAW_PATH}")
        df_analysis.to_csv(OUTPUT_RAW_PATH, index=False)

        logger.info("Interpolating cleaned data...")
        df_analysis_final = interpolate_openaq_data(df_analysis)

        logger.info(f"Saving interpolated data to {OUTPUT_PATH}")
        df_analysis_final.to_csv(OUTPUT_PATH, index=False)
        logger.info("Data cleaning and interpolation complete.")
        
    except Exception as e:
        logger.error(f"Error in cleaning data: {e}")

# === Run ===
if __name__ == "__main__":
    clean_openaq_data()
