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
        report_id = str(uuid.uuid4())
        
        async with AsyncSessionLocal() as session:
            report = Report(
                report_id=report_id,
                status=ReportStatus.RUNNING
            )
            session.add(report)
            await session.commit()
        
        asyncio.create_task(self._generate_report(report_id))
        
        return report_id

    async def get_report_status(self, report_id: str) -> Dict:
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
        try:
            current_time = await self._get_max_timestamp()
            
            store_ids = await self._get_all_store_ids()
            
            report_data = []
            total_stores = len(store_ids)
            validation_errors = []
            
            print(f"Generating report for {total_stores} stores...")
            
            for idx, store_id in enumerate(store_ids):
                try:
                    metrics = await self.uptime_service.calculate_store_metrics(
                        store_id, current_time
                    )
                    
                    # ADDED: Validation to catch mathematical errors
                    validation_result = self._validate_metrics(store_id, metrics)
                    if validation_result:
                        validation_errors.append(validation_result)
                        print(f"Validation warning for store {store_id}: {validation_result}")
                    
                    report_data.append({
                        'store_id': store_id,
                        'uptime_last_hour(in minutes)': metrics.uptime_last_hour,
                        'uptime_last_day(in hours)': metrics.uptime_last_day,
                        'uptime_last_week(in hours)': metrics.uptime_last_week,
                        'downtime_last_hour(in minutes)': metrics.downtime_last_hour,
                        'downtime_last_day(in hours)': metrics.downtime_last_day,
                        'downtime_last_week(in hours)': metrics.downtime_last_week
                    })
                    
                    if (idx + 1) % 100 == 0:
                        print(f"Processed {idx + 1}/{total_stores} stores")
                        
                except Exception as e:
                    print(f"Error processing store {store_id}: {e}")
                    # FIXED: Better error handling with zero values
                    report_data.append({
                        'store_id': store_id,
                        'uptime_last_hour(in minutes)': 0.0,
                        'uptime_last_day(in hours)': 0.0,
                        'uptime_last_week(in hours)': 0.0,
                        'downtime_last_hour(in minutes)': 0.0,
                        'downtime_last_day(in hours)': 0.0,
                        'downtime_last_week(in hours)': 0.0
                    })
                    validation_errors.append(f"Store {store_id}: Processing failed - {str(e)}")
            
            file_path = await self.csv_writer.write_report(report_id, report_data)
            
            # Log validation summary
            if validation_errors:
                print(f"\n=== VALIDATION SUMMARY ===")
                print(f"Total validation issues: {len(validation_errors)}")
                for error in validation_errors[:10]:  # Show first 10 errors
                    print(f"  - {error}")
                if len(validation_errors) > 10:
                    print(f"  ... and {len(validation_errors) - 10} more issues")
                print("=" * 30)
            
            await self._update_report_status(report_id, ReportStatus.COMPLETE, file_path)
            
            print(f"Report {report_id} generated successfully: {file_path}")
            
        except Exception as e:
            print(f"Error generating report {report_id}: {e}")
            await self._update_report_status(report_id, ReportStatus.FAILED, error_message=str(e))

    def _validate_metrics(self, store_id: str, metrics) -> str:
        """Validate that metrics make mathematical sense"""
        
        # Check if hour totals are reasonable (should be close to 60 minutes for most stores)
        hour_total = metrics.uptime_last_hour + metrics.downtime_last_hour
        if hour_total > 60.1 or hour_total < 0:
            return f"Hour total out of range: {hour_total:.2f} minutes"
        
        # Check if day totals are reasonable (should be ≤ 24 hours)
        day_total = metrics.uptime_last_day + metrics.downtime_last_day
        if day_total > 24.1 or day_total < 0:
            return f"Day total out of range: {day_total:.2f} hours"
        
        # Check if week totals are reasonable (should be ≤ 168 hours)
        week_total = metrics.uptime_last_week + metrics.downtime_last_week
        if week_total > 168.1 or week_total < 0:
            return f"Week total out of range: {week_total:.2f} hours"
        
        # Check for negative values
        metrics_dict = {
            'uptime_last_hour': metrics.uptime_last_hour,
            'uptime_last_day': metrics.uptime_last_day,
            'uptime_last_week': metrics.uptime_last_week,
            'downtime_last_hour': metrics.downtime_last_hour,
            'downtime_last_day': metrics.downtime_last_day,
            'downtime_last_week': metrics.downtime_last_week
        }
        
        for metric_name, value in metrics_dict.items():
            if value < 0:
                return f"Negative value in {metric_name}: {value:.2f}"
        
        # Check for suspicious patterns (all zeros)
        if all(value == 0 for value in metrics_dict.values()):
            return "All metrics are zero - possible calculation error"
        
        return None  # No validation errors

    async def _get_max_timestamp(self) -> datetime:
        async with AsyncSessionLocal() as session:
            stmt = select(func.max(StoreStatus.timestamp_utc))
            result = await session.execute(stmt)
            max_timestamp = result.scalar()
            return max_timestamp

    async def _get_all_store_ids(self) -> List[str]:
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