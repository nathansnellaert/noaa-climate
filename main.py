#!/usr/bin/env python3
"""Main orchestrator for NOAA Climate data integration."""

import os
os.environ['CONNECTOR_NAME'] = 'noaa-climate'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.global_temperature_anomalies.global_temperature_anomalies import process_global_temperature_anomalies
from assets.regional_climate_data.regional_climate_data import process_regional_climate_data
from assets.precipitation_data.precipitation_data import process_precipitation_data


def main():
    validate_environment()
    
    # Process global temperature anomaly data (1850-present)
    global_temp_data = process_global_temperature_anomalies()
    if global_temp_data.num_rows > 0:
        upload_data(global_temp_data, "noaa_global_temperature_anomalies")
    
    # Process regional climate summaries with trends
    regional_data = process_regional_climate_data()
    if regional_data.num_rows > 0:
        upload_data(regional_data, "noaa_regional_climate_summaries")
    
    # Process US precipitation and temperature data
    precip_data = process_precipitation_data()
    if precip_data.num_rows > 0:
        upload_data(precip_data, "noaa_us_precipitation")


if __name__ == "__main__":
    main()