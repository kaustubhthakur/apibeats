import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


app = FastAPI(title="FastAPI Heartbeat Manager")

# Connect to Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))


HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "10"))


active_connections = {}


last_heartbeats = {}

class WorkerRegistration(BaseModel):
    worker_id: str
    worker_name: str
    worker_url: str


@app.on_event("startup")
async def startup_event():
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    
    # Start the heartbeat checker task
    asyncio.create_task(check_heartbeats())


@app.on_event("shutdown")
async def shutdown_event():
    await app.state.redis.close()


@app.post("/register")
async def register_worker(worker: WorkerRegistration):
    """Register a new worker with the manager"""
    try:
        worker_data = {
            "worker_id": worker.worker_id,
            "worker_name": worker.worker_name,
            "worker_url": worker.worker_url,
            "registered_at": datetime.now().isoformat(),
            "status": "registered"
        }
        
        
        await app.state.redis.hset(
            f"worker:{worker.worker_id}", 
            mapping=worker_data
        )
        
        logger.info(f"Worker registered: {worker.worker_id} - {worker.worker_name}")
        return {"status": "success", "message": f"Worker {worker.worker_id} registered successfully"}
    
    except Exception as e:
        logger.error(f"Error registering worker: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to register worker: {str(e)}"}
        )


@app.get("/workers")
async def list_workers():
    """List all registered workers and their status"""
    try:
        
        worker_keys = await app.state.redis.keys("worker:*")
        workers = []
        
        for key in worker_keys:
            worker_data = await app.state.redis.hgetall(key)
            
            
            worker_id = worker_data.get("worker_id")
            if worker_id in last_heartbeats:
                last_heartbeat = last_heartbeats[worker_id]
                time_since_last = (datetime.now() - last_heartbeat).total_seconds()
                worker_data["last_heartbeat"] = last_heartbeat.isoformat()
                worker_data["seconds_since_last_heartbeat"] = round(time_since_last, 1)
                worker_data["status"] = "alive" if time_since_last < HEARTBEAT_TIMEOUT else "dead"
            else:
                worker_data["status"] = "registered" if worker_data.get("status") == "registered" else "unknown"
            
            workers.append(worker_data)
        
        return {"workers": workers}
    
    except Exception as e:
        logger.error(f"Error listing workers: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to list workers: {str(e)}"}
        )


@app.websocket("/ws/heartbeat/{worker_id}")
async def websocket_heartbeat(websocket: WebSocket, worker_id: str):
    """WebSocket endpoint to receive heartbeats from workers"""
    await websocket.accept()
    
    
    worker_exists = await app.state.redis.exists(f"worker:{worker_id}")
    if not worker_exists:
        await websocket.close(code=1008, reason="Worker not registered")
        return
    
    
    await app.state.redis.hset(
        f"worker:{worker_id}", 
        "status", 
        "connected"
    )
    
    active_connections[worker_id] = websocket
    logger.info(f"WebSocket connection established with worker: {worker_id}")
    
    try:
        while True:
            
            data = await websocket.receive_text()
            heartbeat_data = json.loads(data)
            
            
            last_heartbeats[worker_id] = datetime.now()
            
            
            heartbeat_key = f"heartbeat:{worker_id}:{datetime.now().isoformat()}"
            await app.state.redis.setex(
                heartbeat_key,
                HEARTBEAT_TIMEOUT * 2,  # TTL for heartbeat data
                json.dumps(heartbeat_data)
            )
            
            
            await websocket.send_text(json.dumps({
                "status": "received",
                "timestamp": datetime.now().isoformat()
            }))
            
            logger.debug(f"Received heartbeat from {worker_id}: {heartbeat_data}")
    
    except WebSocketDisconnect:
        logger.warning(f"WebSocket disconnected for worker: {worker_id}")
        
        
        await app.state.redis.hset(
            f"worker:{worker_id}", 
            "status", 
            "disconnected"
        )
        
        
        if worker_id in active_connections:
            del active_connections[worker_id]
    
    except Exception as e:
        logger.error(f"Error in WebSocket connection with {worker_id}: {str(e)}")
        
        
        await app.state.redis.hset(
            f"worker:{worker_id}", 
            "status", 
            f"error: {str(e)}"
        )
        
        
        if worker_id in active_connections:
            del active_connections[worker_id]


async def check_heartbeats():
    """Background task to check for missing heartbeats"""
    while True:
        try:
            current_time = datetime.now()
            
            
            for worker_id, last_heartbeat in last_heartbeats.items():
                time_since_last = (current_time - last_heartbeat).total_seconds()
                
                if time_since_last > HEARTBEAT_TIMEOUT:
                    logger.warning(f"Worker {worker_id} has not sent a heartbeat in {time_since_last:.1f} seconds")
                    
                    
                    await app.state.redis.hset(
                        f"worker:{worker_id}", 
                        "status", 
                        "dead"
                    )
                    
                   
                    await app.state.redis.hset(
                        f"worker:{worker_id}", 
                        "last_seen", 
                        last_heartbeat.isoformat()
                    )
            
            
            worker_keys = await app.state.redis.keys("worker:*")
            for key in worker_keys:
                worker_id = key.split(":")[1]
                if worker_id not in last_heartbeats:
                    # Get the worker status
                    status = await app.state.redis.hget(key, "status")
                    if status == "registered" or status == "connected":
                        logger.warning(f"Worker {worker_id} is registered but has not sent any heartbeats")
        
        except Exception as e:
            logger.error(f"Error checking heartbeats: {str(e)}")
        
        
        await asyncio.sleep(HEARTBEAT_TIMEOUT / 2)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)