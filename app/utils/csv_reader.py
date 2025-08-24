import pandas as pd
import asyncio
from typing import Dict, Any
import os

class CsvReader:
    
    async def read_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read CSV file asynchronously"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Run pandas read_csv in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, pd.read_csv, file_path)
        
        return df