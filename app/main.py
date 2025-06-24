from fastapi import FastAPI
from app.features.dwd.router import router as dwd_router

app = FastAPI(
    title="ClimaStation API",
    description="Backend API to parse, analyze, and visualize climate data from DWD and beyond.",
    version="0.1.0"
)

app.include_router(dwd_router, prefix="/dwd", tags=["DWD"])

@app.get("/")
def read_root():
    return {"ClimaStation": "Backend is running!"}
