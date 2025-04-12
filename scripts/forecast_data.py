from prophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from logging_util import setup_logger

# === Setup Logging ===
logger = setup_logger("forecast_data", "forecast_data.log")

# === Configuration ===
STATION_FOLDER = "data/stations"
OUTPUT_FOLDER = "outputs/forecasts_prophet"
FORECAST_DAYS = 90

def forecast_station_prophet(station_file):
    try:
        station_name = Path(station_file).stem.replace("_", " ")
        logger.info(f"Processing forecast for {station_name} using Prophet...")

        # Load data
        df = pd.read_csv(station_file, parse_dates=["to_local_date"], index_col="to_local_date")
        if df.empty:
            logger.warning(f"No data available for {station_name}. Skipping...")
            return

        df = df[["summary.avg"]].dropna().reset_index()
        df.columns = ["ds", "y"]

        # Initialize and fit the model
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            seasonality_mode="additive",
            changepoint_prior_scale=0.1
        )
        model.add_country_holidays(country_name='IN')
        model.fit(df)
        logger.info(f"Model fitting complete for {station_name}")

        # Create future data frame
        future = model.make_future_dataframe(periods=FORECAST_DAYS)
        forecast = model.predict(future)

        # Create subfolder for each station
        station_folder = Path(OUTPUT_FOLDER) / station_name.replace(" ", "_")
        station_folder.mkdir(parents=True, exist_ok=True)

        # Plot forecast
        plt.figure(figsize=(10, 6))
        model.plot(forecast)
        plt.title(f"90-Day AQI Forecast - {station_name}")
        plt.xlabel("Date")
        plt.ylabel("AQI")
        plt.tight_layout()
        
        plot_path = station_folder / f"{station_name.replace(' ', '_')}_forecast.png"
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Forecast plot saved for {station_name} at {plot_path}")

        # Plot components (trend, yearly, weekly)
        model.plot_components(forecast)
        plt.title(f"Trend and Seasonality Components - {station_name}")
        
        component_path = station_folder / f"{station_name.replace(' ', '_')}_components.png"
        plt.savefig(component_path)
        plt.close()
        logger.info(f"Component plot saved for {station_name} at {component_path}")

        # Merge forecast with actual values (if available)
        forecast = pd.merge(forecast, df[['ds', 'y']], on='ds', how='left')

        # Save the forecast data to CSV
        output_csv = station_folder / f"{station_name.replace(' ', '_')}_forecast.csv"
        forecast.to_csv(output_csv, index=False)
        logger.info(f"Forecast data saved to {output_csv}")

        print(f"Forecast for {station_name} saved to CSV and PNG.")

    except Exception as e:
        logger.error(f"Error processing {station_file}: {e}")
        print(f"Error processing {station_file}: {e}")

if __name__ == "__main__":
    # Create the output folder if it doesn't exist
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    # Loop through all station files
    station_files = list(Path(STATION_FOLDER).glob("*.csv"))

    for file in station_files:
        forecast_station_prophet(file)

    print("Forecasting complete for all stations!")
