#!/usr/bin/env python3
"""Fetch and process NOAA precipitation data (US-focused)."""

import logging
from datetime import datetime
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NOAA Climate at a Glance API base URL for US data
US_BASE_URL = "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/national/time-series"


def fetch_us_precipitation_data(start_year=1895):
    """Fetch US national precipitation data."""
    current_year = datetime.now().year
    url = f"{US_BASE_URL}/110/pcp/1/12/{start_year}-{current_year}/data.json"
    
    logger.info(f"Fetching US precipitation data ({start_year}-{current_year})...")
    response = get(url, timeout=30.0)
    
    if response.status_code == 404:
        logger.warning(f"No precipitation data available")
        return None
    
    response.raise_for_status()
    return response.json()


def fetch_us_temperature_data(start_year=1895):
    """Fetch US national temperature data for comparison."""
    current_year = datetime.now().year
    url = f"{US_BASE_URL}/110/tavg/1/12/{start_year}-{current_year}/data.json"
    
    logger.info(f"Fetching US temperature data ({start_year}-{current_year})...")
    response = get(url, timeout=30.0)
    
    if response.status_code == 404:
        return None
    
    response.raise_for_status()
    return response.json()


def process_precipitation_data():
    """Process precipitation data (focusing on US national data)."""
    logger.info("Processing NOAA precipitation data...")
    
    # Define schema for the output table
    schema = pa.schema([
        pa.field("year", pa.int32(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("precipitation_inches", pa.float64(), nullable=True),
        pa.field("precipitation_anomaly_inches", pa.float64(), nullable=True),
        pa.field("temperature_fahrenheit", pa.float64(), nullable=True),
        pa.field("temperature_anomaly_fahrenheit", pa.float64(), nullable=True),
        pa.field("data_type", pa.string(), nullable=False),
        pa.field("base_period", pa.string(), nullable=True),
        pa.field("updated_at", pa.timestamp('s'), nullable=False)
    ])
    
    # Load previous state
    state = load_state("precipitation_data")
    last_update = state.get("last_update")
    
    # Check if we've already updated this month
    current_month = datetime.now().strftime("%Y-%m")
    if last_update == current_month:
        logger.info("Data already updated this month, returning empty table")
        return pa.Table.from_pylist([], schema=schema)
    
    records = []
    current_timestamp = datetime.now()
    
    # Fetch US precipitation data
    precip_data = fetch_us_precipitation_data()
    temp_data = fetch_us_temperature_data()
    
    if precip_data:
        # Extract metadata
        precip_desc = precip_data.get("description", {})
        precip_base = precip_desc.get("base_period", "1901-2000")
        
        # Process yearly precipitation data
        yearly_precip = precip_data.get("data", {})
        
        # Get temperature data if available
        yearly_temp = {}
        temp_base = precip_base
        if temp_data:
            temp_desc = temp_data.get("description", {})
            temp_base = temp_desc.get("base_period", "1901-2000")
            yearly_temp = temp_data.get("data", {})
        
        for year_str, precip_values in yearly_precip.items():
            year = int(year_str)
            
            # Get precipitation value (actual value, not anomaly)
            precip_value = precip_values.get("value")
            
            # Calculate anomaly if available
            precip_anomaly = None
            if "anomaly" in precip_values:
                precip_anomaly = precip_values["anomaly"]
            
            # Get temperature data for the same year
            temp_value = None
            temp_anomaly = None
            if year_str in yearly_temp:
                temp_values = yearly_temp[year_str]
                temp_value = temp_values.get("value")
                if "anomaly" in temp_values:
                    temp_anomaly = temp_values["anomaly"]
            
            record = {
                "year": year,
                "region": "United States",
                "precipitation_inches": float(precip_value) if precip_value is not None else None,
                "precipitation_anomaly_inches": float(precip_anomaly) if precip_anomaly is not None else None,
                "temperature_fahrenheit": float(temp_value) if temp_value is not None else None,
                "temperature_anomaly_fahrenheit": float(temp_anomaly) if temp_anomaly is not None else None,
                "data_type": "Annual Average",
                "base_period": precip_base,
                "updated_at": current_timestamp
            }
            records.append(record)
        
        logger.info(f"Processed {len(yearly_precip)} years of US precipitation data")
    
    # Calculate decade averages for trend analysis
    if records:
        decade_records = []
        decades = {}
        
        for record in records:
            decade = (record["year"] // 10) * 10
            if decade not in decades:
                decades[decade] = {
                    "precip_values": [],
                    "temp_values": []
                }
            
            if record["precipitation_inches"] is not None:
                decades[decade]["precip_values"].append(record["precipitation_inches"])
            if record["temperature_fahrenheit"] is not None:
                decades[decade]["temp_values"].append(record["temperature_fahrenheit"])
        
        for decade, values in decades.items():
            if values["precip_values"]:
                avg_precip = sum(values["precip_values"]) / len(values["precip_values"])
                avg_temp = None
                if values["temp_values"]:
                    avg_temp = sum(values["temp_values"]) / len(values["temp_values"])
                
                decade_record = {
                    "year": decade,
                    "region": "United States",
                    "precipitation_inches": round(avg_precip, 2),
                    "precipitation_anomaly_inches": None,
                    "temperature_fahrenheit": round(avg_temp, 2) if avg_temp else None,
                    "temperature_anomaly_fahrenheit": None,
                    "data_type": "Decade Average",
                    "base_period": None,
                    "updated_at": current_timestamp
                }
                decade_records.append(decade_record)
        
        records.extend(decade_records)
        logger.info(f"Added {len(decade_records)} decade average records")
    
    # Save state
    save_state("precipitation_data", {
        "last_update": current_month,
        "records_processed": len(records)
    })
    
    # Create PyArrow table
    table = pa.Table.from_pylist(records, schema=schema)
    logger.info(f"Processed {len(records)} total precipitation records")
    
    return table