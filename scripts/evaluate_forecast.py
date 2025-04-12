from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric
from prophet import Prophet
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from logging_util import setup_logger

# === Setup Logging ===
logger = setup_logger("evaluate_forecast", "evaluate_forecast.log")

# === Configuration ===
STATION_FOLDER = "data/stations"
FORECAST_FOLDER = "outputs/forecasts_prophet"

def evaluate_forecast(file):
    try:
        station_name = Path(file).stem.replace("_", " ")
        logger.info(f"Evaluating forecast for {station_name}...")

        # Load forecast data
        forecast = pd.read_csv(file)

        if forecast.empty:
            logger.warning(f"Forecast data for {station_name} is empty. Skipping evaluation.")
            return

        # Extract actual and predicted values
        actual = forecast['y'].dropna()
        predicted = forecast['yhat'].dropna()

        if len(actual) == 0 or len(predicted) == 0:
            logger.warning(f"No valid actual or predicted values for {station_name}. Skipping.")
            return

        # Calculate MAE, RMSE, MAPE
        mae = np.mean(np.abs(actual - predicted))
        rmse = np.sqrt(np.mean((actual - predicted) ** 2))
        mape = np.mean(np.abs((actual - predicted) / actual.replace(0, np.nan))) * 100

        # Log accuracy metrics
        logger.info(f"{station_name} - MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")
        print(f"{station_name} - MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

        # Cross-Validation using Prophet
        clean_name = station_name.replace(" forecast", "").replace(" ", "_")
        data_path = f"data/stations/{clean_name}.csv"
        if not Path(data_path).exists():
            logger.error(f"Data file for station {station_name} not found: {data_path}")
            return

        df = pd.read_csv(data_path, parse_dates=["to_local_date"])
        df = df[["to_local_date", "summary.avg"]].dropna()
        df.columns = ["ds", "y"]

        model = Prophet(yearly_seasonality=True, 
                        weekly_seasonality=True, 
                        daily_seasonality=False, 
                        seasonality_mode="additive",
                        changepoint_prior_scale=0.1)
        model.add_country_holidays(country_name='IN')
        model.fit(df)
        logger.info(f"Model fitting completed for {station_name}")

        # Cross-validation to evaluate forecast accuracy
        df_cv = cross_validation(model, initial='730 days', period='180 days', horizon='90 days')
        df_performance = performance_metrics(df_cv)

        # Create station-specific output folder
        station_folder = Path(FORECAST_FOLDER) / clean_name
        station_folder.mkdir(parents=True, exist_ok=True)

        # Save cross-validation results
        metrics_path = station_folder / f"{clean_name}_cv_metrics.csv"
        df_performance.to_csv(metrics_path, index=False)
        logger.info(f"Cross-validation metrics saved for {station_name} at {metrics_path}")

        # Plot cross-validation metrics
        plt.figure(figsize=(10, 6))
        plot_cross_validation_metric(df_cv, metric='rmse')
        plt.title(f"RMSE over Forecast Horizon - {station_name}")
        plot_path = station_folder / f"{clean_name}_cv_plot.png"
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Cross-validation plot saved for {station_name} at {plot_path}")

        # Plot trend, weekly, yearly patterns
        fig = model.plot_components(forecast)
        plt.suptitle(f"Trend and Seasonal Components - {station_name}")
        components_path = station_folder / f"{clean_name}_components.png"
        plt.savefig(components_path)
        plt.close()
        logger.info(f"Trend and seasonal components plot saved for {station_name} at {components_path}")

    except Exception as e:
        logger.error(f"Error evaluating {file}: {e}")
        print(f"Error evaluating {file}: {e}")

if __name__ == "__main__":
    try:
        # Create output folder if not exists
        Path(FORECAST_FOLDER).mkdir(parents=True, exist_ok=True)

        # Recursively find forecast CSV files in subdirectories
        forecast_files = list(Path(FORECAST_FOLDER).rglob("*_forecast.csv"))

        if not forecast_files:
            logger.warning("No forecast files found for evaluation.")
            print("No forecast files to evaluate.")
            exit()

        # Process each forecast file
        for file in forecast_files:
            evaluate_forecast(file)

        logger.info("Evaluation and analysis complete for all stations.")
        print("Evaluation and analysis complete for all stations.")
    except Exception as e:
        logger.error(f"Error during evaluation: {e}")
        print(f"Error during evaluation: {e}")
