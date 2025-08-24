import csv
import os
import asyncio
from typing import List, Dict, Any

class CsvWriter:
    
    def __init__(self):
        self.reports_dir = "reports"
        os.makedirs(self.reports_dir, exist_ok=True)

    async def write_report(self, report_id: str, data: List[Dict[str, Any]]) -> str:
        """Write report data to CSV file"""
        file_path = os.path.join(self.reports_dir, f"store_report_{report_id}.csv")
        
        if not data:
            raise ValueError("No data to write to CSV")
        
        # Get field names from first row
        fieldnames = list(data[0].keys())
        
        # Write CSV in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_csv_sync, file_path, data, fieldnames)
        
        return file_path
    
    def _write_csv_sync(self, file_path: str, data: List[Dict[str, Any]], fieldnames: List[str]):
        """Synchronous CSV writing"""
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)