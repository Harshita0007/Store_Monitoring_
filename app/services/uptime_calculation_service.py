from datetime import datetime, timedelta, time, date
from typing import Dict, List, Tuple, Optional
import pytz
from dataclasses import dataclass

from app.config.database import AsyncSessionLocal
from app.models.store_status import StoreStatus
from app.models.business_hours import BusinessHours
from app.models.store_timezone import StoreTimezone
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import selectinload

@dataclass
class UptimeMetrics:
    uptime_last_hour: float  # minutes
    uptime_last_day: float   # hours
    uptime_last_week: float  # hours
    downtime_last_hour: float  # minutes
    downtime_last_day: float   # hours
    downtime_last_week: float  # hours

class UptimeCalculationService:
    
    def __init__(self):
        self.default_timezone = "America/Chicago"

    async def calculate_store_metrics(self, store_id: str, current_time: datetime) -> UptimeMetrics:
        """Calculate uptime/downtime metrics for a specific store"""
        
        async with AsyncSessionLocal() as session:
            # Get store timezone
            timezone_str = await self._get_store_timezone(session, store_id)
            store_tz = pytz.timezone(timezone_str)
            
            # Get business hours
            business_hours = await self._get_business_hours(session, store_id)
            
            # Get status observations within the required time periods
            one_hour_ago = current_time - timedelta(hours=1)
            one_day_ago = current_time - timedelta(days=1)
            one_week_ago = current_time - timedelta(weeks=1)
            
            # Get all status observations for this store within the week
            status_observations = await self._get_status_observations(
                session, store_id, one_week_ago, current_time
            )
            
            # Calculate metrics for each time period
            uptime_last_hour = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_hour_ago, current_time
            )
            
            uptime_last_day = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_day_ago, current_time
            )
            
            uptime_last_week = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_week_ago, current_time
            )
            
            # Calculate total business hours for each period
            total_hours_last_hour = await self._calculate_total_business_hours(
                business_hours, store_tz, one_hour_ago, current_time
            )
            
            total_hours_last_day = await self._calculate_total_business_hours(
                business_hours, store_tz, one_day_ago, current_time
            )
            
            total_hours_last_week = await self._calculate_total_business_hours(
                business_hours, store_tz, one_week_ago, current_time
            )
            
            # Calculate downtime
            downtime_last_hour = max(0, total_hours_last_hour * 60 - uptime_last_hour)
            downtime_last_day = max(0, total_hours_last_day - uptime_last_day)
            downtime_last_week = max(0, total_hours_last_week - uptime_last_week)
            
            return UptimeMetrics(
                uptime_last_hour=uptime_last_hour,
                uptime_last_day=uptime_last_day,
                uptime_last_week=uptime_last_week,
                downtime_last_hour=downtime_last_hour,
                downtime_last_day=downtime_last_day,
                downtime_last_week=downtime_last_week
            )

    async def _get_store_timezone(self, session, store_id: str) -> str:
        """Get timezone for a store, default to America/Chicago if not found"""
        stmt = select(StoreTimezone).where(StoreTimezone.store_id == store_id)
        result = await session.execute(stmt)
        timezone_record = result.scalar_one_or_none()
        
        return timezone_record.timezone_str if timezone_record else self.default_timezone

    async def _get_business_hours(self, session, store_id: str) -> List[BusinessHours]:
        """Get business hours for a store, default to 24/7 if not found"""
        stmt = select(BusinessHours).where(BusinessHours.store_id == store_id)
        result = await session.execute(stmt)
        business_hours = result.scalars().all()
        
        # If no business hours found, assume 24/7 operation
        if not business_hours:
            return self._create_24_7_business_hours(store_id)
        
        return list(business_hours)

    def _create_24_7_business_hours(self, store_id: str) -> List[BusinessHours]:
        """Create 24/7 business hours for a store"""
        business_hours = []
        for day in range(7):  # 0=Monday, 6=Sunday
            bh = BusinessHours()
            bh.store_id = store_id
            bh.day_of_week = day
            bh.start_time_local = time(0, 0, 0)  # 00:00:00
            bh.end_time_local = time(23, 59, 59)  # 23:59:59
            business_hours.append(bh)
        return business_hours

    async def _get_status_observations(
        self, 
        session, 
        store_id: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[StoreStatus]:
        """Get status observations for a store within a time period"""
        stmt = select(StoreStatus).where(
            and_(
                StoreStatus.store_id == store_id,
                StoreStatus.timestamp_utc >= start_time,
                StoreStatus.timestamp_utc <= end_time
            )
        ).order_by(StoreStatus.timestamp_utc)
        
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _calculate_uptime_for_period(
        self,
        status_observations: List[StoreStatus],
        business_hours: List[BusinessHours],
        store_tz: pytz.timezone,
        start_time: datetime,
        end_time: datetime
    ) -> float:
        """
        Calculate uptime for a given period using interpolation logic.
        Returns uptime in minutes for hourly periods, hours for daily/weekly periods.
        """
        
        # Convert UTC times to store local timezone
        start_local = start_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        end_local = end_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        
        total_uptime = 0.0
        current_time = start_local
        
        # Process day by day
        while current_time < end_local:
            day_end = min(
                end_local,
                current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
            
            # Get business hours for this day
            day_business_hours = self._get_business_hours_for_day(
                business_hours, current_time.weekday()
            )
            
            if day_business_hours:
                day_uptime = self._calculate_day_uptime(
                    status_observations, day_business_hours, store_tz,
                    current_time, day_end
                )
                total_uptime += day_uptime
            
            # Move to next day
            current_time = (current_time + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        
        # Convert to appropriate units
        period_hours = (end_time - start_time).total_seconds() / 3600
        if period_hours <= 1:
            return total_uptime * 60  # Return minutes for hourly period
        else:
            return total_uptime  # Return hours for daily/weekly periods

    def _get_business_hours_for_day(
        self, 
        business_hours: List[BusinessHours], 
        weekday: int
    ) -> Optional[BusinessHours]:
        """Get business hours for a specific day (0=Monday, 6=Sunday)"""
        for bh in business_hours:
            if bh.day_of_week == weekday:
                return bh
        return None

    def _calculate_day_uptime(
        self,
        status_observations: List[StoreStatus],
        business_hours: BusinessHours,
        store_tz: pytz.timezone,
        day_start: datetime,
        day_end: datetime
    ) -> float:
        """Calculate uptime for a single day in hours"""
        
        # Get business hours boundaries for this day
        bh_start = day_start.replace(
            hour=business_hours.start_time_local.hour,
            minute=business_hours.start_time_local.minute,
            second=business_hours.start_time_local.second,
            microsecond=0
        )
        bh_end = day_start.replace(
            hour=business_hours.end_time_local.hour,
            minute=business_hours.end_time_local.minute,
            second=business_hours.end_time_local.second,
            microsecond=0
        )
        
        # Handle overnight business hours (e.g., 22:00 to 06:00)
        if business_hours.end_time_local <= business_hours.start_time_local:
            bh_end += timedelta(days=1)
        
        # Constrain to the actual period we're calculating
        period_start = max(bh_start, day_start)
        period_end = min(bh_end, day_end)
        
        if period_start >= period_end:
            return 0.0
        
        # Filter observations within business hours
        business_observations = []
        for obs in status_observations:
            obs_local = obs.timestamp_utc.replace(tzinfo=pytz.UTC).astimezone(store_tz)
            if period_start <= obs_local <= period_end:
                business_observations.append((obs_local, obs.status))
        
        if not business_observations:
            # No observations during business hours, assume store was active
            return (period_end - period_start).total_seconds() / 3600
        
        # Interpolate uptime based on observations
        return self._interpolate_uptime(business_observations, period_start, period_end)

    def _interpolate_uptime(
        self, 
        observations: List[Tuple[datetime, str]], 
        period_start: datetime, 
        period_end: datetime
    ) -> float:
        """
        Interpolate uptime based on status observations.
        
        Logic:
        - If first observation is after period start, assume previous status extends backward
        - If last observation is before period end, assume that status extends forward
        - Between observations, assume status changes at the midpoint
        """
        
        total_duration = (period_end - period_start).total_seconds()
        uptime_seconds = 0.0
        
        # Add period start as a boundary if needed
        if not observations or observations[0][0] > period_start:
            # Assume active status at the beginning if no prior info
            first_status = observations[0][1] if observations else 'active'
            observations.insert(0, (period_start, first_status))
        
        # Add period end as a boundary if needed
        if not observations or observations[-1][0] < period_end:
            # Extend last known status to the end
            last_status = observations[-1][1] if observations else 'active'
            observations.append((period_end, last_status))
        
        # Calculate uptime between consecutive observations
        for i in range(len(observations) - 1):
            current_time, current_status = observations[i]
            next_time, next_status = observations[i + 1]
            
            segment_duration = (next_time - current_time).total_seconds()
            
            if current_status == 'active':
                uptime_seconds += segment_duration
        
        return uptime_seconds / 3600  # Convert to hours

    async def _calculate_total_business_hours(
        self,
        business_hours: List[BusinessHours],
        store_tz: pytz.timezone,
        start_time: datetime,
        end_time: datetime
    ) -> float:
        """Calculate total business hours in a given period"""
        
        start_local = start_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        end_local = end_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        
        total_hours = 0.0
        current_time = start_local
        
        while current_time < end_local:
            day_end = min(
                end_local,
                current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
            
            day_business_hours = self._get_business_hours_for_day(
                business_hours, current_time.weekday()
            )
            
            if day_business_hours:
                bh_start = current_time.replace(
                    hour=day_business_hours.start_time_local.hour,
                    minute=day_business_hours.start_time_local.minute,
                    second=day_business_hours.start_time_local.second,
                    microsecond=0
                )
                bh_end = current_time.replace(
                    hour=day_business_hours.end_time_local.hour,
                    minute=day_business_hours.end_time_local.minute,
                    second=day_business_hours.end_time_local.second,
                    microsecond=0
                )
                
                # Handle overnight hours
                if day_business_hours.end_time_local <= day_business_hours.start_time_local:
                    bh_end += timedelta(days=1)
                
                # Calculate overlap with the period
                period_start = max(bh_start, current_time)
                period_end = min(bh_end, day_end)
                
                if period_start < period_end:
                    total_hours += (period_end - period_start).total_seconds() / 3600
            
            # Move to next day
            current_time = (current_time + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        
        return total_hours
