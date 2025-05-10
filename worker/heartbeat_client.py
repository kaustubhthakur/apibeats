import asyncio
import logging
import os
import uuid
from fastapi import FastAPI, BackgroundTasks
from heartbeat_client import HeartbeatClient


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("worker")


MANAGER_URL = os.getenv("MANAGER_URL", "ws://manager:8000")
WORKER_ID = os.getenv("WORKER_ID", str(uuid.uuid4()))
WORKER_NAME = os.getenv("WORKER_NAME", f"worker-{WORKER_ID}")
SERVICE_TYPE = os.getenv("SERVICE_TYPE", "demo-service")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 10))  # seconds

app = FastAPI(title="Demo Worker Service")
heartbeat_client = None


@app.on_event("startup")
async def startup_event():
    global heartbeat_client
    heartbeat_client = HeartbeatClient(
        manager_url=MANAGER_URL,
        worker_id=WORKER_ID,
        worker_name=WORKER_NAME,
        service_type=SERVICE_TYPE,
        metadata={"version": "1.0.0"},
        heartbeat_interval=HEARTBEAT_INTERVAL,
        on_disconnect=on_disconnect,
        on_reconnect=on_reconnect
    )
    await heartbeat_client.start()
    logger.info(f"Worker started with ID: {WORKER_ID}")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop the heartbeat client
    if heartbeat_client:
        await heartbeat_client.stop()
    logger.info("Worker shutting down")


async def on_disconnect():
    logger.warning("Disconnected from heartbeat manager")


async def on_reconnect():
    logger.info("Reconnected to heartbeat manager")


@app.get("/")
async def root():
    """Simple health check endpoint"""
    return {"status": "online", "worker_id": WORKER_ID}


@app.get("/status")
async def status():
    """Get worker status"""
    return {
        "worker_id": WORKER_ID,
        "name": WORKER_NAME,
        "service_type": SERVICE_TYPE,
        "heartbeat_connected": heartbeat_client.connected if heartbeat_client else False
    }


@app.post("/simulate/failure")
async def simulate_failure(background_tasks: BackgroundTasks):
    """Endpoint to simulate a heartbeat failure by stopping the client"""
    async def stop_heartbeat():
        global heartbeat_client
        if heartbeat_client:
            await heartbeat_client.stop()
            logger.info("Heartbeat client stopped to simulate failure")
    
    background_tasks.add_task(stop_heartbeat)
    return {"message": "Simulating heartbeat failure"}


@app.post("/simulate/recovery")
async def simulate_recovery(background_tasks: BackgroundTasks):
    """Endpoint to simulate recovery by restarting the heartbeat client"""
    async def restart_heartbeat():
        global heartbeat_client
        if heartbeat_client:
            await heartbeat_client.stop()
            await heartbeat_client.start()
            logger.info("Heartbeat client restarted")
    
    background_tasks.add_task(restart_heartbeat)
    return {"message": "Simulating recovery"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)