import pandas as pd
import asyncio
from datetime import datetime, time
from typing import List
import pytz
import os

from app.config.database import AsyncSessionLocal
from app.models.store_status import StoreStatus
from app.models.business_hours import BusinessHours
from app.models.store_timezone import StoreTimezone
from app.utils.csv_reader import CsvReader
from sqlalchemy import text

class DataIngestionService:
    def __init__(self):
        self.csv_reader = CsvReader()

    async def load_all_data(self):
        """Load all CSV data into the database"""
        print("Starting data ingestion...")
        
        # Initialize database first
        from app.config.database import init_db
        await init_db()
        
        # Load data sequentially to avoid SQLite locking issues
        await self.load_store_status_data()
        await self.load_business_hours_data()
        await self.load_timezone_data()
        
        print("Data ingestion completed!")

    async def load_store_status_data(self):
        """Load store status data from CSV"""
        try:
            df = await self.csv_reader.read_csv("data/store_status.csv")
            
            async with AsyncSessionLocal() as session:
                # Clear existing data (ignore if table doesn't exist)
                try:
                    await session.execute(text("DELETE FROM store_status"))
                except Exception:
                    pass  # Table might not exist yet
                
                batch_size = 1000
                total_rows = len(df)
                
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:i + batch_size]
                    status_objects = []
                    
                    for _, row in batch.iterrows():
                        # Parse timestamp
                        timestamp_utc = pd.to_datetime(row['timestamp_utc'])
                        
                        status_obj = StoreStatus(
                            store_id=str(row['store_id']),
                            timestamp_utc=timestamp_utc,
                            status=row['status']
                        )
                        status_objects.append(status_obj)
                    
                    session.add_all(status_objects)
                    await session.commit()
                    
                    print(f"Loaded {min(i + batch_size, total_rows)} / {total_rows} store status records")
                
        except Exception as e:
            print(f"Error loading store status data: {e}")
            raise

    async def load_business_hours_data(self):
        """Load business hours data from CSV"""
        try:
            # Check if file exists
            if not os.path.exists("data/menu_hours.csv"):
                print("Business hours file not found, stores will be assumed 24/7")
                return

            df = await self.csv_reader.read_csv("data/menu_hours.csv")
            
            async with AsyncSessionLocal() as session:
                # Clear existing data (ignore if table doesn't exist)
                try:
                    await session.execute(text("DELETE FROM business_hours"))
                except Exception:
                    pass  # Table might not exist yet
                
                business_hours_objects = []
                
                for _, row in df.iterrows():
                    # Parse time strings
                    start_time = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
                    end_time = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
                    
                    business_hours_obj = BusinessHours(
                        store_id=str(row['store_id']),
                        day_of_week=int(row['dayOfWeek']),
                        start_time_local=start_time,
                        end_time_local=end_time
                    )
                    business_hours_objects.append(business_hours_obj)
                
                session.add_all(business_hours_objects)
                await session.commit()
                
                print(f"Loaded {len(business_hours_objects)} business hours records")
                
        except Exception as e:
            print(f"Error loading business hours data: {e}")
            raise

    async def load_timezone_data(self):
        """Load timezone data from CSV"""
        try:
            # Check if file exists
            if not os.path.exists("data/timezones.csv"):
                print("Timezone file not found, stores will use America/Chicago")
                return

            df = await self.csv_reader.read_csv("data/timezones.csv")
            
            async with AsyncSessionLocal() as session:
                # Clear existing data (ignore if table doesn't exist)
                try:
                    await session.execute(text("DELETE FROM store_timezone"))
                except Exception:
                    pass  # Table might not exist yet
                
                timezone_objects = []
                
                for _, row in df.iterrows():
                    timezone_obj = StoreTimezone(
                        store_id=str(row['store_id']),
                        timezone_str=row['timezone_str']
                    )
                    timezone_objects.append(timezone_obj)
                
                session.add_all(timezone_objects)
                await session.commit()
                
                print(f"Loaded {len(timezone_objects)} timezone records")
                
        except Exception as e:
            print(f"Error loading timezone data: {e}")
            raise
