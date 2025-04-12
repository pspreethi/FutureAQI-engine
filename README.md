# FutureAQI-engine
Backend workflow for AQI forecasting using OpenAQ API. Includes scripts for data fetching, filtering, cleaning, interpolation, forecasting with Facebook Prophet, and evaluation via cross-validation. Modular, reproducible, and designed to support the futureAQI-forecast app.

---
## Air Quality Forecasting Scripts

This repository contains the backend workflow for air quality forecasting using data from the OpenAQ API. It includes modular Python scripts for fetching, cleaning, transforming, forecasting, and evaluating PM2.5 sensor data across Indian cities using time-series modeling.

---

## Setup Instructions

1. Clone the Repository

```bash
git clone https://github.com/your-username/air-quality-scripts.git
cd air-quality-scripts
```
2. Install Dependencies
```bash
pip install -r requirements.txt
```
3. Configure API Key
Copy the template environment file and rename it:

```bash
cp config_template.env .env
```
Then, open .env and add your OpenAQ API key:

OPENAQ_API_KEY=your_actual_api_key

## Usage Guide
1. Fetch Data
```bash
python scripts/fetch_data.py
```

- Fetches all Indian PM2.5 sensor metadata and daily readings.

2. Clean & Interpolate Data
```bash
python scripts/clean_data.py
```
- Cleans, filters, and interpolates missing or invalid values.

3. Split by Station
```bash
python scripts/split_by_station.py
```
- Creates one daily .csv file for each valid sensor station.

4. Exploratory Data Analysis
```bash
python scripts/station_eda.py
```
- Generates per-station trend and seasonality plots.

5. Forecast Using Prophet
```bash
python scripts/forecast_data.py
```
- Builds Prophet models to forecast the next 90 days of AQI per station.

6. Evaluate Forecast Accuracy
```bash
python scripts/evaluate_forecast.py
```
- Performs error calculation and Prophet cross-validation.

### Run All in One Go
```bash
python scripts/pipeline.py
```

## Environment & Logging

- API key is stored securely using .env
- Logs are created automatically during execution (e.g., fetch_data.log)

## Key Dependencies

- `pandas`
- `requests`
- `python-dotenv`
- `prophet`
- `matplotlib`, `seaborn`
- `scikit-learn`

## Acknowledgements

- Data Source: [OpenAQ API](https://docs.openaq.org/)
- Forecasting: [Facebook Prophet](https://facebook.github.io/prophet/)

## License

This project is licensed under the MIT License.

