from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse
import uvicorn
import asyncio
from contextlib import asynccontextmanager

from app.config.database import init_db
from app.controllers.report_controller import router as report_router
from app.services.data_ingestion_service import DataIngestionService

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    
    # Initialize data ingestion in background (non-blocking)
    async def load_data_background():
        try:
            ingestion_service = DataIngestionService()
            await ingestion_service.load_all_data()
        except Exception as e:
            print(f"Data ingestion failed: {e}")
    
    # Start data ingestion in background
    asyncio.create_task(load_data_background())
    
    yield
    
    # Shutdown
    pass

app = FastAPI(
    title="Store Monitoring System",
    description="API for monitoring store uptime and generating reports",
    version="1.0.0",
    lifespan=lifespan
)

# Include routers
app.include_router(report_router, prefix="/api/v1")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Store Monitoring System is running"}

@app.get("/")
async def root():
    return {"message": "Store Monitoring System API", "docs": "/docs"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
