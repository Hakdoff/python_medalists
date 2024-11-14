import os
import pandas as pd
import logging
import time
import asyncio
import threading
import queue
from motor.motor_asyncio import AsyncIOMotorClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import InsertOne
from pathlib import Path
from datetime import datetime

UPLOAD_DIR = "storage/app/medalists/"
ARCHIVE_DIR = "storage/app/archive/"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = AsyncIOMotorClient("mongodb://localhost:27017") 
db = client["medalist_database"]

class CSVHandler(FileSystemEventHandler):
    def __init__(self, task_queue):
        super().__init__()
        self.task_queue = task_queue

    def on_created(self, event):
        if event.is_directory:
            return
        elif event.src_path.endswith(".csv"):
            logging.info(f"Detected new CSV file: {event.src_path}")
            self.task_queue.put(event.src_path)

class AsyncProcessor:
    def __init__(self):
        self.loop = None
        self.running = True

    def get_unique_archive_path(self, original_path):
        """
        Generate a unique archive path by appending timestamp to the original filename.
        """
        base_path = Path(ARCHIVE_DIR)
        original_name = Path(original_path).name
        name_parts = original_name.rsplit('.', 1)
        base_name = name_parts[0]
        extension = name_parts[1] if len(name_parts) > 1 else ''
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{base_name}_{timestamp}.{extension}"
        return base_path / new_name

    async def process_csv(self, file_path):
        try:
            await asyncio.sleep(1)

            logging.info(f"Starting to process: {file_path}")
            df = pd.read_csv(file_path, encoding='ISO-8859-1')

            required_columns = {"name", "medal_type", "gender", "country", "country_code", 
                                "nationality", "medal_code", "medal_date"}

            if not required_columns.issubset(df.columns):
                logging.error(f"CSV file {file_path} is missing required columns")
                return

            operations = []
            processed_count = 0
            for _, row in df.iterrows():
                document = {column: (None if pd.isna(value) else value) for column, value in row.items()}
                
                logging.info(f"Checking document: {document}")
                
                query = {"name": document["name"], "medal_date": document["medal_date"]}
                logging.info(f"Checking if record exists with query: {query}")
                
                existing = await db.medalists.find_one(query)
                logging.info(f"Existing record found: {existing is not None}")

                if not existing:
                    operations.append(InsertOne(document))
                    processed_count += 1

            if operations:
                result = await db.medalists.bulk_write(operations)
                logging.info(f"Processed {processed_count} records: Inserted {result.inserted_count} new records.")

                archive_dir = Path(ARCHIVE_DIR)
                archive_dir.mkdir(parents=True, exist_ok=True)

                if os.path.exists(file_path):
                    archive_path = self.get_unique_archive_path(file_path)
                    os.rename(file_path, archive_path)
                    logging.info(f"Moved {file_path} to {archive_path}")
                else:
                    logging.error(f"File {file_path} does not exist, skipping move.")
            else:
                logging.info(f"No new records to insert for file: {file_path}")

        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            logging.info(f"File {file_path} remains in the input directory due to processing error.")
            
    async def process_queue(self, task_queue):
        while self.running:
            try:
                file_path = task_queue.get_nowait()
                await self.process_csv(file_path)
                task_queue.task_done()
            except queue.Empty:
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Error in process_queue: {str(e)}")

    def run(self, task_queue):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            self.loop.run_until_complete(self.process_queue(task_queue))
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            pending = asyncio.all_tasks(self.loop)
            self.loop.run_until_complete(asyncio.gather(*pending))
            self.loop.close()

def run_watcher(directory_to_watch, task_queue):
    event_handler = CSVHandler(task_queue)
    observer = Observer()
    observer.schedule(event_handler, directory_to_watch, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def start_background_service():
    Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)

    task_queue = queue.Queue()

    processor = AsyncProcessor()

    watcher_thread = threading.Thread(target=run_watcher, args=(UPLOAD_DIR, task_queue))
    watcher_thread.daemon = True

    processor_thread = threading.Thread(target=processor.run, args=(task_queue,))
    processor_thread.daemon = True

    watcher_thread.start()
    processor_thread.start()

    return watcher_thread, processor_thread, processor

if __name__ == "__main__":
    watcher_thread, processor_thread, processor = start_background_service()
    logging.info("Background service started, monitoring directory for CSV files.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down service...")
        processor.running = False
