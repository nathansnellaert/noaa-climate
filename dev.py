import os

# Set environment variables for testing
os.environ['CONNECTOR_NAME'] = 'noaa-climate'
os.environ['RUN_ID'] = 'test-full-connector'
os.environ['ENABLE_HTTP_CACHE'] = 'true'  # Cache responses for faster testing
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

print("Testing NOAA Climate connector...")
print("="*60)

# Test individual assets first
print("\n1. Testing global temperature anomalies...")
from assets.global_temperature_anomalies.global_temperature_anomalies import process_global_temperature_anomalies

try:
    global_data = process_global_temperature_anomalies()
    print(f"✓ Global temperature data: {global_data.num_rows} rows")
    if global_data.num_rows > 0:
        print(f"  Columns: {global_data.column_names}")
        print(f"  Sample data:")
        sample = global_data.slice(0, 3).to_pandas()
        print(sample)
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "="*60)
print("\n2. Testing regional climate summaries...")
from assets.regional_climate_data.regional_climate_data import process_regional_climate_data

try:
    regional_data = process_regional_climate_data()
    print(f"✓ Regional climate data: {regional_data.num_rows} rows")
    if regional_data.num_rows > 0:
        print(f"  Columns: {regional_data.column_names}")
        print(f"  Sample data:")
        sample = regional_data.slice(0, 3).to_pandas()
        print(sample)
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "="*60)
print("\n3. Testing precipitation data...")
from assets.precipitation_data.precipitation_data import process_precipitation_data

try:
    precip_data = process_precipitation_data()
    print(f"✓ Precipitation data: {precip_data.num_rows} rows")
    if precip_data.num_rows > 0:
        print(f"  Columns: {precip_data.column_names}")
        print(f"  Sample data:")
        sample = precip_data.slice(0, 3).to_pandas()
        print(sample)
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "="*60)
print("\n4. Testing full orchestration...")
from main import main

try:
    main()
    print("✓ Full orchestration completed successfully!")
except Exception as e:
    print(f"✗ Orchestration error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("\nConnector test complete!")