import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Set, Optional

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from pydantic import BaseModel


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("heartbeat-manager")


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", 30))  # seconds

app = FastAPI(title="Heartbeat Manager")


active_connections: Dict[str, WebSocket] = {}

last_heartbeats: Dict[str, datetime] = {}


class WorkerInfo(BaseModel):
    worker_id: str
    name: str
    service_type: str
    metadata: dict = {}


@app.on_event("startup")
async def startup_event():
  
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    

    asyncio.create_task(check_heartbeats())
    logger.info("Heartbeat Manager started")


@app.on_event("shutdown")
async def shutdown_event():
   
    await app.state.redis.close()
    logger.info("Heartbeat Manager shutting down")


async def check_heartbeats():
    """Background task to check for missing heartbeats"""
    while True:
        await asyncio.sleep(HEARTBEAT_TIMEOUT // 2)
        current_time = datetime.utcnow()
        dead_workers = []
        
        for worker_id, last_time in list(last_heartbeats.items()):
            if current_time - last_time > timedelta(seconds=HEARTBEAT_TIMEOUT):
                dead_workers.append(worker_id)
                logger.warning(f"Worker {worker_id} missed heartbeat, last seen: {last_time}")
                
               
                last_heartbeats.pop(worker_id, None)
                
                
                worker_info = await app.state.redis.hget("workers", worker_id)
                if worker_info:
                    worker_data = json.loads(worker_info)
                    worker_data["status"] = "offline"
                    worker_data["last_seen"] = last_time.isoformat()
                    await app.state.redis.hset("workers", worker_id, json.dumps(worker_data))


@app.websocket("/ws/register")
async def register_worker(websocket: WebSocket):
    await websocket.accept()
    worker_id = None
    
    try:
       
        registration_data = await websocket.receive_json()
        worker_id = registration_data.get("worker_id")
        
        if not worker_id:
            await websocket.send_json({"status": "error", "message": "worker_id is required"})
            await websocket.close()
            return
        
        
        active_connections[worker_id] = websocket
        
        
        worker_data = {
            "worker_id": worker_id,
            "name": registration_data.get("name", f"worker-{worker_id}"),
            "service_type": registration_data.get("service_type", "unknown"),
            "metadata": registration_data.get("metadata", {}),
            "status": "online",
            "registered_at": datetime.utcnow().isoformat(),
            "last_heartbeat": datetime.utcnow().isoformat()
        }
        
        await app.state.redis.hset("workers", worker_id, json.dumps(worker_data))
        logger.info(f"Worker registered: {worker_id}")
        
       
        await websocket.send_json({"status": "registered", "worker_id": worker_id})
        
        
        while True:
            heartbeat_data = await websocket.receive_json()
            current_time = datetime.utcnow()
            last_heartbeats[worker_id] = current_time
            
            
            worker_info = await app.state.redis.hget("workers", worker_id)
            if worker_info:
                worker_data = json.loads(worker_info)
                worker_data["last_heartbeat"] = current_time.isoformat()
                worker_data["status"] = "online"
                await app.state.redis.hset("workers", worker_id, json.dumps(worker_data))
            
            logger.debug(f"Heartbeat received from {worker_id}")
            
            await websocket.send_json({"status": "heartbeat_ack", "timestamp": current_time.isoformat()})
            
    except WebSocketDisconnect:
        logger.info(f"Worker disconnected: {worker_id}")
        if worker_id:
            
            active_connections.pop(worker_id, None)
            last_heartbeats.pop(worker_id, None)
            
           
            worker_info = await app.state.redis.hget("workers", worker_id)
            if worker_info:
                worker_data = json.loads(worker_info)
                worker_data["status"] = "offline"
                worker_data["last_seen"] = datetime.utcnow().isoformat()
                await app.state.redis.hset("workers", worker_id, json.dumps(worker_data))
    
    except Exception as e:
        logger.error(f"Error in websocket connection: {str(e)}")
        if worker_id:
            active_connections.pop(worker_id, None)
            last_heartbeats.pop(worker_id, None)


@app.get("/workers")
async def list_workers():
    """List all registered workers and their statuses"""
    workers = {}
    worker_keys = await app.state.redis.hgetall("workers")
    
    for worker_id, worker_data in worker_keys.items():
        workers[worker_id] = json.loads(worker_data)
    
    return {"workers": workers}


@app.get("/workers/{worker_id}")
async def get_worker(worker_id: str):
    """Get info about a specific worker"""
    worker_data = await app.state.redis.hget("workers", worker_id)
    if not worker_data:
        return {"status": "error", "message": "Worker not found"}
    
    return {"worker": json.loads(worker_data)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)