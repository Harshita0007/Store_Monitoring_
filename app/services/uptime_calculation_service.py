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
    uptime_last_hour: float  
    uptime_last_day: float   
    uptime_last_week: float  
    downtime_last_hour: float  
    downtime_last_day: float   
    downtime_last_week: float  

class UptimeCalculationService:
    
    def __init__(self):
        self.default_timezone = "America/Chicago"

    async def calculate_store_metrics(self, store_id: str, current_time: datetime) -> UptimeMetrics:
        
        async with AsyncSessionLocal() as session:
            timezone_str = await self._get_store_timezone(session, store_id)
            store_tz = pytz.timezone(timezone_str)
            
            business_hours = await self._get_business_hours(session, store_id)
            
            one_hour_ago = current_time - timedelta(hours=1)
            one_day_ago = current_time - timedelta(days=1)
            one_week_ago = current_time - timedelta(weeks=1)
            
            status_observations = await self._get_status_observations(
                session, store_id, one_week_ago, current_time )
            
            # FIXED: All calculations now return hours consistently
            uptime_last_hour_hours = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_hour_ago, current_time )
            
            uptime_last_day_hours = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_day_ago, current_time )
            
            uptime_last_week_hours = await self._calculate_uptime_for_period(
                status_observations, business_hours, store_tz, one_week_ago, current_time )
            
            total_hours_last_hour = await self._calculate_total_business_hours(
                business_hours, store_tz, one_hour_ago, current_time)
            
            total_hours_last_day = await self._calculate_total_business_hours(
                business_hours, store_tz, one_day_ago, current_time )
            
            total_hours_last_week = await self._calculate_total_business_hours(
                business_hours, store_tz, one_week_ago, current_time)
            
            # FIXED: Consistent unit calculations
            downtime_last_hour_hours = max(0, total_hours_last_hour - uptime_last_hour_hours)
            downtime_last_day_hours = max(0, total_hours_last_day - uptime_last_day_hours)
            downtime_last_week_hours = max(0, total_hours_last_week - uptime_last_week_hours)
            
            # FIXED: Better rounding to avoid floating point issues
            return UptimeMetrics(
                uptime_last_hour=self._safe_round(uptime_last_hour_hours * 60, 2),  # Convert to minutes
                uptime_last_day=self._safe_round(uptime_last_day_hours, 2),
                uptime_last_week=self._safe_round(uptime_last_week_hours, 2),
                downtime_last_hour=self._safe_round(downtime_last_hour_hours * 60, 2),  # Convert to minutes
                downtime_last_day=self._safe_round(downtime_last_day_hours, 2),
                downtime_last_week=self._safe_round(downtime_last_week_hours, 2)
            )

    def _safe_round(self, value: float, decimals: int = 2) -> float:
        """Round with better floating point handling"""
        return round(float(value), decimals)

    async def _get_store_timezone(self, session, store_id: str) -> str:
        stmt = select(StoreTimezone).where(StoreTimezone.store_id == store_id)
        result = await session.execute(stmt)
        timezone_record = result.scalar_one_or_none()
        
        return timezone_record.timezone_str if timezone_record else self.default_timezone

    async def _get_business_hours(self, session, store_id: str) -> List[BusinessHours]:
        stmt = select(BusinessHours).where(BusinessHours.store_id == store_id)
        result = await session.execute(stmt)
        business_hours = result.scalars().all()
        
        if not business_hours:
            return self._create_24_7_business_hours(store_id)
        
        return list(business_hours)

    def _create_24_7_business_hours(self, store_id: str) -> List[BusinessHours]:
        business_hours = []
        for day in range(7):  
            bh = BusinessHours()
            bh.store_id = store_id
            bh.day_of_week = day
            bh.start_time_local = time(0, 0, 0)  
            bh.end_time_local = time(23, 59, 59)  
            business_hours.append(bh)
        return business_hours

    async def _get_status_observations(
        self, 
        session, 
        store_id: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[StoreStatus]:
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
        """FIXED: Always returns hours regardless of period length"""
        
        start_local = start_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        end_local = end_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        
        total_uptime = 0.0
        current_time = start_local
        
        while current_time < end_local:
            # FIXED: Use proper date boundaries instead of artificial day-end times
            next_day = (current_time + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = min(end_local, next_day)
            
            day_business_hours = self._get_business_hours_for_day(
                business_hours, current_time.weekday()
            )
            
            if day_business_hours:
                day_uptime = self._calculate_day_uptime(
                    status_observations, day_business_hours, store_tz,
                    current_time, day_end
                )
                total_uptime += day_uptime
            
            current_time = next_day
        
        # FIXED: Always return hours (removed the conditional unit conversion)
        return total_uptime

    def _get_business_hours_for_day(
        self, 
        business_hours: List[BusinessHours], 
        weekday: int
    ) -> Optional[BusinessHours]:
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
        """FIXED: Better handling of overnight business hours"""
        
        # Get the date for this calculation
        calculation_date = day_start.date()
        
        # Create business hours boundaries for this specific date
        bh_start = store_tz.localize(datetime.combine(
            calculation_date, business_hours.start_time_local
        ))
        bh_end = store_tz.localize(datetime.combine(
            calculation_date, business_hours.end_time_local
        ))
        
        # Handle overnight business hours (e.g., 22:00 - 06:00 next day)
        if business_hours.end_time_local <= business_hours.start_time_local:
            bh_end += timedelta(days=1)
        
        # Find the intersection of business hours with our calculation period
        period_start = max(bh_start, day_start)
        period_end = min(bh_end, day_end)
        
        if period_start >= period_end:
            return 0.0
        
        # Get observations that fall within business hours for this period
        business_observations = []
        for obs in status_observations:
            obs_local = obs.timestamp_utc.replace(tzinfo=pytz.UTC).astimezone(store_tz)
            if period_start <= obs_local <= period_end:
                business_observations.append((obs_local, obs.status))
        
        if not business_observations:
            # No observations during business hours - assume active (default behavior)
            return (period_end - period_start).total_seconds() / 3600
        
        return self._interpolate_uptime(business_observations, period_start, period_end)

    def _interpolate_uptime(
        self, 
        observations: List[Tuple[datetime, str]], 
        period_start: datetime, 
        period_end: datetime
    ) -> float:
        """FIXED: Improved interpolation logic with better boundary handling"""
        
        if not observations:
            return (period_end - period_start).total_seconds() / 3600
        
        total_duration = (period_end - period_start).total_seconds()
        uptime_seconds = 0.0
        
        # Sort observations by timestamp
        observations.sort(key=lambda x: x[0])
        
        # Extend observations to cover the entire period
        extended_observations = observations.copy()
        
        # If first observation is after period start, extend backwards
        if observations[0][0] > period_start:
            # Use the first observation's status for the beginning
            extended_observations.insert(0, (period_start, observations[0][1]))
        
        # If last observation is before period end, extend forwards
        if observations[-1][0] < period_end:
            # Use the last observation's status for the end
            extended_observations.append((period_end, observations[-1][1]))
        
        # Calculate uptime for each segment
        for i in range(len(extended_observations) - 1):
            current_time, current_status = extended_observations[i]
            next_time, _ = extended_observations[i + 1]
            
            # Ensure we're within our calculation period
            segment_start = max(current_time, period_start)
            segment_end = min(next_time, period_end)
            
            if segment_start < segment_end and current_status == 'active':
                uptime_seconds += (segment_end - segment_start).total_seconds()
        
        return uptime_seconds / 3600

    async def _calculate_total_business_hours(
        self,
        business_hours: List[BusinessHours],
        store_tz: pytz.timezone,
        start_time: datetime,
        end_time: datetime
    ) -> float:
        """FIXED: Improved business hours calculation with better date handling"""
        
        start_local = start_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        end_local = end_time.replace(tzinfo=pytz.UTC).astimezone(store_tz)
        
        total_hours = 0.0
        current_date = start_local.date()
        end_date = end_local.date()
        
        # Process each day in the period
        while current_date <= end_date:
            day_business_hours = self._get_business_hours_for_day(
                business_hours, current_date.weekday()
            )
            
            if not day_business_hours:
                current_date += timedelta(days=1)
                continue
            
            # Create day boundaries
            day_start = store_tz.localize(datetime.combine(current_date, time.min))
            day_end = store_tz.localize(datetime.combine(current_date, time.max))
            
            # Intersect with the period we're calculating
            period_start_for_day = max(start_local, day_start)
            period_end_for_day = min(end_local, day_end)
            
            if period_start_for_day >= period_end_for_day:
                current_date += timedelta(days=1)
                continue
            
            # Calculate business hours for this day
            bh_start = store_tz.localize(datetime.combine(
                current_date, day_business_hours.start_time_local
            ))
            bh_end = store_tz.localize(datetime.combine(
                current_date, day_business_hours.end_time_local
            ))
            
            # Handle overnight business hours
            if day_business_hours.end_time_local <= day_business_hours.start_time_local:
                bh_end += timedelta(days=1)
            
            # Find intersection of business hours with our calculation period for this day
            effective_start = max(bh_start, period_start_for_day)
            effective_end = min(bh_end, period_end_for_day)
            
            if effective_start < effective_end:
                total_hours += (effective_end - effective_start).total_seconds() / 3600
            
            current_date += timedelta(days=1)
        
        return total_hours