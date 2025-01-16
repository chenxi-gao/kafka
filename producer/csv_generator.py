import pandas as pd
import numpy as np
import time
import os
from datetime import datetime

def generate_row():
    """Generate a single row of sample data"""
    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'temperature': round(np.random.uniform(20, 30), 2),
        'humidity': round(np.random.uniform(40, 80), 2),
        'pressure': round(np.random.uniform(980, 1020), 2),
        'wind_speed': round(np.random.uniform(0, 20), 2)
    }

def write_data():
    # Get the path to the listener directory
    listener_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_listener')
    os.makedirs(listener_dir, exist_ok=True)
    
    csv_path = os.path.join(listener_dir, 'test.csv')
    
    # If file doesn't exist, create it with headers
    if not os.path.exists(csv_path):
        df = pd.DataFrame([generate_row()])
        df.to_csv(csv_path, index=False)
        print(f"Created new file: {csv_path}")
    
    while True:
        try:
            # Read existing data
            df = pd.read_csv(csv_path)
            
            # Generate new row
            new_row = generate_row()
            
            # Append new row
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            # Keep only last 100 rows to prevent file from growing too large
            if len(df) > 100:
                df = df.tail(100)
            
            # Write back to file
            df.to_csv(csv_path, index=False)
            
            print(f"Added new data at {new_row['timestamp']}")
            
            # Wait for 5 seconds before next update
            time.sleep(5)
            
        except Exception as e:
            print(f"Error writing data: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    print("Starting data generator...")
    print("Press Ctrl+C to stop")
    write_data() 