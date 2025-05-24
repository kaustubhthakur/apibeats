import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Set
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkerRegistration(BaseModel):
    worker_id: str
    worker_name: str
    host: str
    port: int
    description: str = ""

class HeartbeatManager:
    def __init__(self):
        self.redis_client = None
        self.active_connections: Dict[str, WebSocket] = {}
        self.worker_info: Dict[str, dict] = {}
        self.last_heartbeat: Dict[str, datetime] = {}
        self.heartbeat_timeout = 30  # seconds
        
    async def init_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host='redis', 
                port=6379, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def register_worker(self, registration: WorkerRegistration):
        """Register a new worker"""
        try:
            worker_data = {
                "worker_id": registration.worker_id,
                "worker_name": registration.worker_name,
                "host": registration.host,
                "port": registration.port,
                "description": registration.description,
                "registered_at": datetime.now().isoformat(),
                "status": "registered"
            }
            
            # Store in Redis
            await self.redis_client.hset(
                f"worker:{registration.worker_id}", 
                mapping=worker_data
            )
            
            # Store in memory
            self.worker_info[registration.worker_id] = worker_data
            
            logger.info(f"Worker {registration.worker_id} ({registration.worker_name}) registered successfully")
            return {"status": "success", "message": "Worker registered successfully"}
            
        except Exception as e:
            logger.error(f"Failed to register worker {registration.worker_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to register worker")
    
    async def handle_websocket_connection(self, websocket: WebSocket, worker_id: str):
        """Handle WebSocket connection for heartbeats"""
        try:
            await websocket.accept()
            logger.info(f"WebSocket connection accepted for worker {worker_id}")
            
            # Check if worker is registered
            worker_data = await self.redis_client.hgetall(f"worker:{worker_id}")
            if not worker_data:
                logger.warning(f"Worker {worker_id} not registered, closing connection")
                await websocket.close(code=4004, reason="Worker not registered")
                return
            
            self.active_connections[worker_id] = websocket
            self.last_heartbeat[worker_id] = datetime.now()
            
            # Update worker status to connected
            await self.redis_client.hset(f"worker:{worker_id}", "status", "connected")
            
            logger.info(f"Worker {worker_id} connected via WebSocket")
            
            while True:
                try:
                    # Wait for heartbeat message
                    data = await websocket.receive_text()
                    heartbeat_data = json.loads(data)
                    
                    if heartbeat_data.get("type") == "heartbeat":
                        await self.process_heartbeat(worker_id, heartbeat_data)
                        
                except WebSocketDisconnect:
                    logger.warning(f"Worker {worker_id} disconnected")
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON from worker {worker_id}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message from worker {worker_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error handling WebSocket for worker {worker_id}: {e}")
        finally:
            await self.cleanup_worker_connection(worker_id)
    
    async def process_heartbeat(self, worker_id: str, heartbeat_data: dict):
        """Process received heartbeat"""
        current_time = datetime.now()
        self.last_heartbeat[worker_id] = current_time
        
        # Update Redis with latest heartbeat
        await self.redis_client.hset(
            f"worker:{worker_id}", 
            mapping={
                "last_heartbeat": current_time.isoformat(),
                "status": "active",
                "health_data": json.dumps(heartbeat_data.get("health", {}))
            }
        )
        
        logger.debug(f"Processed heartbeat from worker {worker_id}")
        
        # Send acknowledgment
        if worker_id in self.active_connections:
            try:
                await self.active_connections[worker_id].send_text(
                    json.dumps({
                        "type": "heartbeat_ack",
                        "timestamp": current_time.isoformat(),
                        "status": "received"
                    })
                )
                logger.debug(f"Sent heartbeat ack to worker {worker_id}")
            except Exception as e:
                logger.error(f"Failed to send heartbeat ack to {worker_id}: {e}")
    
    async def cleanup_worker_connection(self, worker_id: str):
        """Clean up worker connection"""
        if worker_id in self.active_connections:
            del self.active_connections[worker_id]
        
        if worker_id in self.last_heartbeat:
            del self.last_heartbeat[worker_id]
        
        # Update status in Redis
        try:
            await self.redis_client.hset(f"worker:{worker_id}", "status", "disconnected")
        except Exception as e:
            logger.error(f"Failed to update Redis status for worker {worker_id}: {e}")
            
        logger.info(f"Cleaned up connection for worker {worker_id}")
    
    async def check_worker_health(self):
        """Background task to check worker health"""
        while True:
            try:
                current_time = datetime.now()
                timeout_threshold = current_time - timedelta(seconds=self.heartbeat_timeout)
                
                # Check all registered workers
                worker_keys = await self.redis_client.keys("worker:*")
                
                for key in worker_keys:
                    worker_data = await self.redis_client.hgetall(key)
                    worker_id = worker_data.get("worker_id")
                    
                    if not worker_id:
                        continue
                    
                    last_heartbeat_str = worker_data.get("last_heartbeat")
                    current_status = worker_data.get("status", "registered")
                    
                    if last_heartbeat_str:
                        try:
                            last_heartbeat = datetime.fromisoformat(last_heartbeat_str)
                            
                            if last_heartbeat < timeout_threshold and current_status not in ["timeout", "disconnected"]:
                                # Worker is considered down
                                await self.redis_client.hset(key, "status", "timeout")
                                logger.warning(
                                    f"Worker {worker_id} ({worker_data.get('worker_name')}) "
                                    f"has not sent heartbeat for {self.heartbeat_timeout} seconds - marked as timeout"
                                )
                        except ValueError as e:
                            logger.error(f"Invalid datetime format for worker {worker_id}: {e}")
                    elif current_status == "registered":
                        # Worker registered but never sent heartbeat - only warn after some time
                        registered_at_str = worker_data.get("registered_at")
                        if registered_at_str:
                            try:
                                registered_at = datetime.fromisoformat(registered_at_str)
                                if current_time - registered_at > timedelta(seconds=60):  # Wait 60 seconds before warning
                                    logger.warning(f"Worker {worker_id} is registered but has not sent any heartbeats")
                            except ValueError:
                                pass
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in health check: {e}")
                await asyncio.sleep(10)
    
    async def get_workers_status(self):
        """Get status of all workers"""
        try:
            worker_keys = await self.redis_client.keys("worker:*")
            workers_status = []
            
            for key in worker_keys:
                worker_data = await self.redis_client.hgetall(key)
                if worker_data:
                    workers_status.append(worker_data)
            
            return workers_status
        except Exception as e:
            logger.error(f"Failed to get workers status: {e}")
            return []
    
    async def unregister_worker(self, worker_id: str):
        """Unregister a worker"""
        try:
            # Remove from Redis
            await self.redis_client.delete(f"worker:{worker_id}")
            
            # Clean up connections
            await self.cleanup_worker_connection(worker_id)
            
            # Remove from memory
            if worker_id in self.worker_info:
                del self.worker_info[worker_id]
            
            logger.info(f"Worker {worker_id} unregistered successfully")
            return {"status": "success", "message": "Worker unregistered successfully"}
            
        except Exception as e:
            logger.error(f"Failed to unregister worker {worker_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to unregister worker")

# Initialize FastAPI app and manager
app = FastAPI(title="Heartbeat Manager", version="1.0.0")
manager = HeartbeatManager()

@app.on_event("startup")
async def startup_event():
    """Initialize manager on startup"""
    await manager.init_redis()
    # Start health check background task
    asyncio.create_task(manager.check_worker_health())

@app.post("/register")
async def register_worker(registration: WorkerRegistration):
    """Register a new worker"""
    return await manager.register_worker(registration)

@app.delete("/unregister/{worker_id}")
async def unregister_worker(worker_id: str):
    """Unregister a worker"""
    return await manager.unregister_worker(worker_id)

@app.get("/workers")
async def get_workers():
    """Get all workers status"""
    workers = await manager.get_workers_status()
    return {"workers": workers, "count": len(workers)}

@app.get("/workers/{worker_id}")
async def get_worker(worker_id: str):
    """Get specific worker status"""
    worker_data = await manager.redis_client.hgetall(f"worker:{worker_id}")
    if not worker_data:
        raise HTTPException(status_code=404, detail="Worker not found")
    return worker_data

# Fixed WebSocket endpoint - make sure path matches what worker expects
@app.websocket("/ws/heartbeat/{worker_id}")
async def websocket_heartbeat(websocket: WebSocket, worker_id: str):
    """WebSocket endpoint for receiving heartbeats"""
    await manager.handle_websocket_connection(websocket, worker_id)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_connections": len(manager.active_connections),
        "registered_workers": len(await manager.get_workers_status())
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)