import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer
import os
import json
from datetime import datetime
import pandas as pd
from queue import Queue
import threading

# Kafka configuration with acknowledgment settings
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,  # Ensure exactly-once delivery
    'acks': 'all',              # Wait for all replicas
    'retries': 5,               # Number of retries if sending fails
    'retry.backoff.ms': 500,    # Time between retries
    'delivery.timeout.ms': 10000  # Maximum time to wait for delivery
}

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self.producer = Producer(kafka_config)
        self.last_row_count = 0
        self.last_data_hash = None
        self.pending_messages = Queue()
        self.failed_messages = Queue()
        
        # Start background thread for handling failed messages
        self.retry_thread = threading.Thread(target=self._retry_failed_messages, daemon=True)
        self.retry_thread.start()

    def _delivery_callback(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
            # Store failed message for retry
            self.failed_messages.put({
                'topic': msg.topic(),
                'key': msg.key(),
                'value': msg.value(),
                'retries': 0
            })
        else:
            # Message was delivered successfully
            self.pending_messages.get()
            self.pending_messages.task_done()
            print(f"Message delivered successfully to {msg.topic()}")

    def _retry_failed_messages(self):
        while True:
            try:
                if not self.failed_messages.empty():
                    failed_msg = self.failed_messages.get()
                    if failed_msg['retries'] < 5:  # Maximum retry attempts
                        print(f"Retrying message delivery, attempt {failed_msg['retries'] + 1}")
                        self.producer.produce(
                            failed_msg['topic'],
                            key=failed_msg['key'],
                            value=failed_msg['value'],
                            callback=self._delivery_callback
                        )
                        failed_msg['retries'] += 1
                        self.producer.poll(0)  # Trigger delivery reports
                    else:
                        print(f"Message failed after maximum retries: {failed_msg}")
                        # Here you could implement additional error handling
                        # like saving to a dead letter queue
                time.sleep(1)
            except Exception as e:
                print(f"Error in retry thread: {e}")

    def on_modified(self, event):
        if event.is_directory:
            return
            
        # Only process CSV files
        if not event.src_path.lower().endswith('.csv'):
            return
        
        try:
            # Add a small delay to ensure file is completely written
            time.sleep(0.1)
            
            # Read the CSV file using pandas
            df = pd.read_csv(event.src_path)
            
            # If this is the first read
            if self.last_row_count == 0:
                self.last_row_count = len(df)
                return
            
            # Check if we have new rows
            current_row_count = len(df)
            if current_row_count > self.last_row_count:
                # Get only the new rows
                new_rows = df.iloc[self.last_row_count:current_row_count]
                
                # Convert new rows to JSON string
                new_data = new_rows.to_json(orient='records')
                
                # Prepare message with metadata
                message = {
                    'filename': os.path.basename(event.src_path),
                    'data': new_data,
                    'timestamp': datetime.now().isoformat(),
                    'event_type': 'modified',
                    'new_rows_count': len(new_rows),
                    'total_rows': current_row_count,
                    'columns': df.columns.tolist(),
                    'start_index': self.last_row_count,  # Add index information for tracking
                    'end_index': current_row_count
                }
                
                # Add to pending messages
                self.pending_messages.put(message)
                
                # Send to Kafka topic with delivery confirmation
                self.producer.produce(
                    'csv_updates',
                    key=os.path.basename(event.src_path),
                    value=json.dumps(message),
                    callback=self._delivery_callback
                )
                
                # Trigger any available delivery report callbacks
                self.producer.poll(0)
                
                print(f"Sent {len(new_rows)} new rows from file: {event.src_path}")
                
                # Only update counter after successful send
                self.last_row_count = current_row_count
            
        except Exception as e:
            print(f"Error processing CSV file {event.src_path}: {str(e)}")

    def wait_for_pending_messages(self):
        """Wait for all pending messages to be delivered"""
        print("Waiting for pending messages to be delivered...")
        self.pending_messages.join()
        self.producer.flush()

def start_watching():
    # Path to monitor
    path_to_watch = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_listener')
    
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=False)
    observer.start()
    
    print(f"Started watching directory for CSV files: {path_to_watch}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping file watcher...")
        # Wait for pending messages before stopping
        event_handler.wait_for_pending_messages()
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    start_watching() 