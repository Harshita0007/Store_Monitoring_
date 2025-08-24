# Store Monitoring System

A FastAPI-based system for monitoring store uptime and generating reports.

## Features

- Store status monitoring
- Business hours tracking
- Timezone support
- Uptime/downtime calculations
- Report generation
- RESTful API

## Setup

1. **Install dependencies:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the application:**
   ```bash
   python run.py
   ```

The application will start on `http://localhost:8001`

## API Endpoints

### Health Check

- `GET /health` - Check if the system is running

### Reports

- `POST /api/v1/trigger_report` - Trigger a new report generation
- `GET /api/v1/get_report?report_id={id}` - Get report status or download completed report

### Documentation

- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /openapi.json` - OpenAPI specification

## Data Files

The system expects the following CSV files in the `data/` directory:

- `store_status.csv` - Store status records with columns: store_id, timestamp_utc, status
- `menu_hours.csv` - Business hours with columns: store_id, dayOfWeek, start_time_local, end_time_local
- `timezones.csv` - Store timezones with columns: store_id, timezone_str

## Database

The system uses SQLite as the database (stored in `store_monitoring.db`). The database is automatically created and initialized when the application starts.

## Usage Example

1. Start the application:

   ```bash
   python run.py
   ```

2. Trigger a report:

   ```bash
   curl -X POST http://localhost:8001/api/v1/trigger_report
   ```

3. Check report status:

   ```bash
   curl "http://localhost:8001/api/v1/get_report?report_id=YOUR_REPORT_ID"
   ```

4. View API documentation:
   Open `http://localhost:8001/docs` in your browser

## Project Structure

```
store-monitoring/
├── app/
│   ├── config/          # Database configuration
│   ├── controllers/     # API endpoints
│   ├── models/          # Database models
│   ├── services/        # Business logic
│   └── utils/           # Utility functions
├── data/                # CSV data files
├── reports/             # Generated report files
├── requirements.txt     # Python dependencies
└── run.py              # Application entry point
```

## Notes

- The application loads data in the background during startup
- Large datasets may take some time to process
- Reports are generated asynchronously
- The system automatically handles timezone conversions
