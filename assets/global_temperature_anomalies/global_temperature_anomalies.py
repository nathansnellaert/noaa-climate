#!/usr/bin/env python3
"""Fetch and process NOAA global temperature anomaly data."""

import logging
from datetime import datetime
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NOAA Climate at a Glance API base URL
BASE_URL = "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series"


def fetch_temperature_data(region, surface_type, start_year=1850):
    """Fetch temperature anomaly data for a specific region and surface type."""
    current_year = datetime.now().year
    url = f"{BASE_URL}/{region}/{surface_type}/1/12/{start_year}-{current_year}/data.json"
    
    logger.info(f"Fetching {region} {surface_type} temperature data...")
    response = get(url, timeout=30.0)
    
    if response.status_code == 404:
        logger.warning(f"No data available for {region} {surface_type}")
        return None
    
    response.raise_for_status()
    return response.json()


def process_global_temperature_anomalies():
    """Process global temperature anomaly data and return as PyArrow table."""
    logger.info("Processing NOAA global temperature anomalies...")
    
    # Define schema for the output table
    schema = pa.schema([
        pa.field("year", pa.int32(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("surface_type", pa.string(), nullable=False),
        pa.field("temperature_anomaly_celsius", pa.float64(), nullable=False),
        pa.field("base_period", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("data_source", pa.string(), nullable=False),
        pa.field("updated_at", pa.timestamp('s'), nullable=False)
    ])
    
    # Load previous state
    state = load_state("global_temperature_anomalies")
    last_update = state.get("last_update")
    
    # Check if we've already updated today (data updates monthly)
    today = datetime.now().strftime("%Y-%m-%d")
    if last_update == today:
        logger.info("Data already updated today, returning empty table")
        return pa.Table.from_pylist([], schema=schema)
    
    # Define regions and surface types to fetch
    regions = [
        ("globe", "Global"),
        ("nhem", "Northern Hemisphere"),
        ("shem", "Southern Hemisphere"),
        ("europe", "Europe"),
        ("asia", "Asia"),
        ("africa", "Africa")
    ]
    
    surface_types = [
        ("land_ocean", "Land and Ocean"),
        ("land", "Land"),
        ("ocean", "Ocean")
    ]
    
    records = []
    current_timestamp = datetime.now()
    
    for region_code, region_name in regions:
        for surface_code, surface_name in surface_types:
            # Skip ocean-only data for continental regions
            if region_code in ["europe", "asia", "africa"] and surface_code == "ocean":
                continue
            
            data = fetch_temperature_data(region_code, surface_code)
            
            if data is None:
                continue
            
            # Extract metadata
            description = data.get("description", {})
            title = description.get("title", "")
            base_period = description.get("base_period", "1901-2000")
            
            # Process yearly data
            yearly_data = data.get("data", {})
            for year_str, values in yearly_data.items():
                anomaly = values.get("anomaly")
                if anomaly is not None:
                    record = {
                        "year": int(year_str),
                        "region": region_name,
                        "surface_type": surface_name,
                        "temperature_anomaly_celsius": float(anomaly),
                        "base_period": base_period,
                        "description": title,
                        "data_source": "NOAA Climate at a Glance",
                        "updated_at": current_timestamp
                    }
                    records.append(record)
            
            logger.info(f"Processed {len(yearly_data)} years for {region_name} - {surface_name}")
    
    # Save state
    save_state("global_temperature_anomalies", {
        "last_update": today,
        "records_processed": len(records)
    })
    
    # Create PyArrow table
    table = pa.Table.from_pylist(records, schema=schema)
    logger.info(f"Processed {len(records)} temperature anomaly records")
    
    return table