from confluent_kafka import Consumer, KafkaError
import json
import os
from datetime import datetime
import pandas as pd
import tempfile
import shutil

# Kafka configuration with consumer group settings
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'csv_gatherer_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,  # Disable auto commit
    'max.poll.interval.ms': 300000,  # 5 minutes
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 10000
}

class CSVConsumer:
    def __init__(self):
        self.consumer = Consumer(kafka_config)
        self.last_processed_indices = {}  # Track processed indices per file

    def _safe_file_write(self, df, output_path):
        """Safely write DataFrame to file using a temporary file"""
        # Create a temporary file in the same directory
        temp_dir = os.path.dirname(output_path)
        with tempfile.NamedTemporaryFile(mode='w', dir=temp_dir, delete=False) as temp_file:
            temp_path = temp_file.name
            try:
                # Write to temporary file
                df.to_csv(temp_path, index=False)
                # Atomically replace the target file
                shutil.move(temp_path, output_path)
            except Exception as e:
                # Clean up the temporary file if something goes wrong
                os.unlink(temp_path)
                raise e

    def _is_duplicate_message(self, filename, start_index, end_index):
        """Check if message contains already processed indices"""
        if filename not in self.last_processed_indices:
            self.last_processed_indices[filename] = set()
            return False
        
        # Check if any of the indices in the range have been processed
        new_indices = set(range(start_index, end_index))
        if new_indices & self.last_processed_indices[filename]:
            return True
        
        # Update processed indices
        self.last_processed_indices[filename].update(new_indices)
        return False

    def save_to_file(self, data):
        # Create data_gather directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_gather')
        os.makedirs(output_dir, exist_ok=True)
        
        # Parse the message
        message = json.loads(data)
        original_filename = message['filename']
        new_rows_data = json.loads(message['data'])
        start_index = message.get('start_index', 0)
        end_index = message.get('end_index', 0)
        
        # Check for duplicate message
        if self._is_duplicate_message(original_filename, start_index, end_index):
            print(f"Skipping duplicate data for indices {start_index}-{end_index}")
            return
        
        # Set output path
        output_path = os.path.join(output_dir, original_filename)
        
        try:
            # Convert new rows to DataFrame
            new_df = pd.DataFrame(new_rows_data)
            
            # If file exists, append to it; otherwise create new
            if os.path.exists(output_path):
                existing_df = pd.read_csv(output_path)
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                updated_df = new_df
            
            # Safely write the updated DataFrame
            self._safe_file_write(updated_df, output_path)
            
            print(f"Added {len(new_df)} rows to: {output_path} (Total rows: {len(updated_df)})")
            
        except Exception as e:
            print(f"Error saving CSV data: {e}")
            raise e

    def start_consuming(self):
        self.consumer.subscribe(['csv_updates'])
        
        print("Started consuming CSV messages...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break
                
                try:
                    # Process message
                    self.save_to_file(msg.value().decode('utf-8'))
                    # Commit offset only after successful processing
                    self.consumer.commit(msg)
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    # Here you could implement additional error handling
                    # like dead letter queue or retry mechanism
                    
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        finally:
            # Make sure to close consumer cleanly
            self.consumer.close()

if __name__ == "__main__":
    consumer = CSVConsumer()
    consumer.start_consuming() 