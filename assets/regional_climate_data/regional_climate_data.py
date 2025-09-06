#!/usr/bin/env python3
"""Fetch and process NOAA regional climate summary data with recent trends."""

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


def fetch_recent_climate_data(region, surface_type, years_back=10):
    """Fetch recent climate data for trend analysis."""
    current_year = datetime.now().year
    start_year = current_year - years_back
    url = f"{BASE_URL}/{region}/{surface_type}/1/12/{start_year}-{current_year}/data.json"
    
    logger.info(f"Fetching recent {region} {surface_type} data ({start_year}-{current_year})...")
    response = get(url, timeout=30.0)
    
    if response.status_code == 404:
        logger.warning(f"No recent data for {region} {surface_type}")
        return None
    
    response.raise_for_status()
    return response.json()


def calculate_trend_statistics(yearly_data):
    """Calculate trend statistics from yearly temperature anomaly data."""
    if not yearly_data:
        return None
    
    years = sorted(yearly_data.keys())
    anomalies = [yearly_data[year].get("anomaly", 0) for year in years]
    
    # Calculate statistics
    avg_anomaly = sum(anomalies) / len(anomalies)
    min_anomaly = min(anomalies)
    max_anomaly = max(anomalies)
    
    # Calculate simple linear trend (change per decade)
    if len(anomalies) >= 2:
        first_half_avg = sum(anomalies[:len(anomalies)//2]) / (len(anomalies)//2)
        second_half_avg = sum(anomalies[len(anomalies)//2:]) / (len(anomalies) - len(anomalies)//2)
        trend_direction = "warming" if second_half_avg > first_half_avg else "cooling"
        trend_magnitude = abs(second_half_avg - first_half_avg)
    else:
        trend_direction = "stable"
        trend_magnitude = 0.0
    
    return {
        "avg_anomaly": round(avg_anomaly, 3),
        "min_anomaly": round(min_anomaly, 3),
        "max_anomaly": round(max_anomaly, 3),
        "trend_direction": trend_direction,
        "trend_magnitude": round(trend_magnitude, 3),
        "latest_anomaly": round(anomalies[-1], 3) if anomalies else None,
        "latest_year": int(years[-1]) if years else None
    }


def process_regional_climate_data():
    """Process regional climate summary data with statistics and trends."""
    logger.info("Processing NOAA regional climate summaries...")
    
    # Define schema for the output table
    schema = pa.schema([
        pa.field("region", pa.string(), nullable=False),
        pa.field("surface_type", pa.string(), nullable=False),
        pa.field("period_start", pa.int32(), nullable=False),
        pa.field("period_end", pa.int32(), nullable=False),
        pa.field("avg_temperature_anomaly", pa.float64(), nullable=False),
        pa.field("min_temperature_anomaly", pa.float64(), nullable=False),
        pa.field("max_temperature_anomaly", pa.float64(), nullable=False),
        pa.field("latest_year", pa.int32(), nullable=True),
        pa.field("latest_anomaly", pa.float64(), nullable=True),
        pa.field("trend_direction", pa.string(), nullable=False),
        pa.field("trend_magnitude_celsius", pa.float64(), nullable=False),
        pa.field("base_period", pa.string(), nullable=True),
        pa.field("data_points", pa.int32(), nullable=False),
        pa.field("updated_at", pa.timestamp('s'), nullable=False)
    ])
    
    # Load previous state
    state = load_state("regional_climate_data")
    last_update = state.get("last_update")
    
    # Check if we've already updated this month
    current_month = datetime.now().strftime("%Y-%m")
    if last_update == current_month:
        logger.info("Data already updated this month, returning empty table")
        return pa.Table.from_pylist([], schema=schema)
    
    # Define regions and analysis periods
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
    
    periods = [10, 30, 50]  # Recent decade, 30 years, 50 years
    
    records = []
    current_timestamp = datetime.now()
    current_year = current_timestamp.year
    
    for region_code, region_name in regions:
        for surface_code, surface_name in surface_types:
            # Skip ocean-only data for continental regions
            if region_code in ["europe", "asia", "africa"] and surface_code == "ocean":
                continue
            
            for period in periods:
                data = fetch_recent_climate_data(region_code, surface_code, period)
                
                if data is None:
                    continue
                
                # Extract metadata
                description = data.get("description", {})
                base_period = description.get("base_period", "1901-2000")
                
                # Calculate statistics
                yearly_data = data.get("data", {})
                stats = calculate_trend_statistics(yearly_data)
                
                if stats:
                    record = {
                        "region": region_name,
                        "surface_type": surface_name,
                        "period_start": current_year - period,
                        "period_end": current_year,
                        "avg_temperature_anomaly": stats["avg_anomaly"],
                        "min_temperature_anomaly": stats["min_anomaly"],
                        "max_temperature_anomaly": stats["max_anomaly"],
                        "latest_year": stats["latest_year"],
                        "latest_anomaly": stats["latest_anomaly"],
                        "trend_direction": stats["trend_direction"],
                        "trend_magnitude_celsius": stats["trend_magnitude"],
                        "base_period": base_period,
                        "data_points": len(yearly_data),
                        "updated_at": current_timestamp
                    }
                    records.append(record)
            
            logger.info(f"Processed climate summaries for {region_name} - {surface_name}")
    
    # Save state
    save_state("regional_climate_data", {
        "last_update": current_month,
        "records_processed": len(records)
    })
    
    # Create PyArrow table
    table = pa.Table.from_pylist(records, schema=schema)
    logger.info(f"Processed {len(records)} regional climate summary records")
    
    return table