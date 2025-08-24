from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import FileResponse
import os
from typing import Dict

from app.services.report_service import ReportService

router = APIRouter()
report_service = ReportService()

@router.post("/trigger_report")
async def trigger_report() -> Dict[str, str]:
   
    try:
        report_id = await report_service.trigger_report()
        return {"report_id": report_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger report: {str(e)}")

@router.get("/get_report")
async def get_report(report_id: str):
    try:
        result = await report_service.get_report_status(report_id)
        
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        
        if result["status"] == "Complete":
            file_path = result["file_path"]
            if os.path.exists(file_path):
                return FileResponse(
                    path=file_path,
                    filename=f"store_report_{report_id}.csv",
                    media_type="text/csv"
                )
            else:
                raise HTTPException(status_code=404, detail="Report file not found")
        
        return {"status": result["status"]}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")
