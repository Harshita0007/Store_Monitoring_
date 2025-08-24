import asyncio
import uuid
import csv
from datetime import datetime
from typing import List, Dict
import os

from app.config.database import AsyncSessionLocal
from app.models.report import Report, ReportStatus
from app.models.store_status import StoreStatus
from app.services.uptime_calculation_service import UptimeCalculationService
from app.utils.csv_writer import CsvWriter
from sqlalchemy import select, func

class ReportService:
    
    def __init__(self):
        self.uptime_service = UptimeCalculationService()
        self.csv_writer = CsvWriter()

    async def trigger_report(self) -> str:
        """Trigger a new report generation and return report_id"""
        report_id = str(uuid.uuid4())
        
        async with AsyncSessionLocal() as session:
            # Create report record
            report = Report(
                report_id=report_id,
                status=ReportStatus.RUNNING
            )
            session.add(report)
            await session.commit()
        
        # Start report generation in background
        asyncio.create_task(self._generate_report(report_id))
        
        return report_id

    async def get_report_status(self, report_id: str) -> Dict:
        """Get report status and file if complete"""
        async with AsyncSessionLocal() as session:
            stmt = select(Report).where(Report.report_id == report_id)
            result = await session.execute(stmt)
            report = result.scalar_one_or_none()
            
            if not report:
                return {"error": "Report not found"}
            
            if report.status == ReportStatus.COMPLETE:
                return {
                    "status": report.status,
                    "file_path": report.file_path
                }
            elif report.status == ReportStatus.FAILED:
                return {
                    "status": report.status,
                    "error": report.error_message
                }
            else:
                return {"status": report.status}

    async def _generate_report(self, report_id: str):
        """Generate the uptime/downtime report"""
        try:
            # Get current timestamp (max timestamp from data)
            current_time = await self._get_max_timestamp()
            
            # Get all unique store IDs
            store_ids = await self._get_all_store_ids()
            
            # Calculate metrics for each store
            report_data = []
            total_stores = len(store_ids)
            
            print(f"Generating report for {total_stores} stores...")
            
            for idx, store_id in enumerate(store_ids):
                try:
                    metrics = await self.uptime_service.calculate_store_metrics(
                        store_id, current_time
                    )
                    
                    report_data.append({
                        'store_id': store_id,
                        'uptime_last_hour(in minutes)': round(metrics.uptime_last_hour, 2),
                        'uptime_last_day(in hours)': round(metrics.uptime_last_day, 2),
                        'uptime_last_week(in hours)': round(metrics.uptime_last_week, 2),
                        'downtime_last_hour(in minutes)': round(metrics.downtime_last_hour, 2),
                        'downtime_last_day(in hours)': round(metrics.downtime_last_day, 2),
                        'downtime_last_week(in hours)': round(metrics.downtime_last_week, 2)
                    })
                    
                    if (idx + 1) % 100 == 0:
                        print(f"Processed {idx + 1}/{total_stores} stores")
                        
                except Exception as e:
                    print(f"Error processing store {store_id}: {e}")
                    # Add empty metrics for failed stores
                    report_data.append({
                        'store_id': store_id,
                        'uptime_last_hour(in minutes)': 0.0,
                        'uptime_last_day(in hours)': 0.0,
                        'uptime_last_week(in hours)': 0.0,
                        'downtime_last_hour(in minutes)': 0.0,
                        'downtime_last_day(in hours)': 0.0,
                        'downtime_last_week(in hours)': 0.0
                    })
            
            # Write report to CSV
            file_path = await self.csv_writer.write_report(report_id, report_data)
            
            # Update report status
            await self._update_report_status(report_id, ReportStatus.COMPLETE, file_path)
            
            print(f"Report {report_id} generated successfully: {file_path}")
            
        except Exception as e:
            print(f"Error generating report {report_id}: {e}")
            await self._update_report_status(report_id, ReportStatus.FAILED, error_message=str(e))

    async def _get_max_timestamp(self) -> datetime:
        """Get the maximum timestamp from store status data"""
        async with AsyncSessionLocal() as session:
            stmt = select(func.max(StoreStatus.timestamp_utc))
            result = await session.execute(stmt)
            max_timestamp = result.scalar()
            return max_timestamp

    async def _get_all_store_ids(self) -> List[str]:
        """Get all unique store IDs from status data"""
        async with AsyncSessionLocal() as session:
            stmt = select(StoreStatus.store_id).distinct()
            result = await session.execute(stmt)
            store_ids = [row[0] for row in result.fetchall()]
            return store_ids

    async def _update_report_status(
        self, 
        report_id: str, 
        status: ReportStatus, 
        file_path: str = None, 
        error_message: str = None
    ):
        """Update report status in database"""
        async with AsyncSessionLocal() as session:
            stmt = select(Report).where(Report.report_id == report_id)
            result = await session.execute(stmt)
            report = result.scalar_one()
            
            report.status = status
            if file_path:
                report.file_path = file_path
            if error_message:
                report.error_message = error_message
            
            await session.commit()
