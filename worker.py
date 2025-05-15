import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
import signal
import sys

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4()}")
WORKER_NAME = os.getenv("WORKER_NAME", f"FastAPI Worker {WORKER_ID}")
WORKER_PORT = int(os.getenv("WORKER_PORT", "8080"))
MANAGER_URL = os.getenv("MANAGER_URL", "http://manager:8000")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))  # seconds


@asynccontextmanager
async def lifespan(app: FastAPI):
   
    try:
        await register_with_manager()
        
        app.heartbeat_task = asyncio.create_task(send_heartbeats())
        yield
        
        app.heartbeat_task.cancel()
        try:
            await app.heartbeat_task
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
    except Exception as e:
        logger.error(f"Error during worker setup: {str(e)}")
        yield



app = FastAPI(
    title=f"FastAPI Worker {WORKER_ID}",
    lifespan=lifespan
)


manager_ws = None
heartbeat_count = 0


async def register_with_manager():
    """Register this worker with the manager"""
    logger.info(f"Registering worker {WORKER_ID} with manager at {MANAGER_URL}")
    
    registration_data = {
        "worker_id": WORKER_ID,
        "worker_name": WORKER_NAME,
        "worker_url": f"http://localhost:{WORKER_PORT}"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{MANAGER_URL}/register", 
                json=registration_data
            )
            
            if response.status_code == 200:
                logger.info("Worker registered successfully with manager")
                return True
            else:
                logger.error(f"Failed to register with manager: {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"Error connecting to manager: {str(e)}")
            return False


async def connect_to_manager_ws():
    """Establish WebSocket connection to manager for heartbeats"""
    global manager_ws
    
    
    if manager_ws:
        try:
            await manager_ws.close()
        except:
            pass
    
    try:
        
        ws_url = f"{MANAGER_URL.replace('http', 'ws')}/ws/heartbeat/{WORKER_ID}"
        logger.info(f"Connecting to WebSocket at {ws_url}")
        
        async with httpx.AsyncClient() as client:
            manager_ws = await client.websocket(ws_url)
            logger.info("WebSocket connection established with manager")
            return True
    
    except Exception as e:
        logger.error(f"Failed to connect to manager WebSocket: {str(e)}")
        manager_ws = None
        return False


async def send_heartbeats():
    """Send periodic heartbeats to manager"""
    global heartbeat_count
    
    while True:
        try:
            if not manager_ws:
                success = await connect_to_manager_ws()
                if not success:
                    
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                    continue
            
           
            heartbeat_count += 1
            
            
            heartbeat_data = {
                "worker_id": WORKER_ID,
                "timestamp": datetime.now().isoformat(),
                "count": heartbeat_count,
                "status": "healthy",
                "metrics": {
                    "memory_usage": get_memory_usage(),
                    "cpu_usage": get_cpu_usage()
                }
            }
            
            # Send heartbeat
            await manager_ws.send(json.dumps(heartbeat_data))
            logger.debug(f"Sent heartbeat #{heartbeat_count}")
            
            # Wait for acknowledgment
            response = await manager_ws.receive()
            response_data = json.loads(response)
            logger.debug(f"Received acknowledgment: {response_data}")
            
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        
        except WebSocketDisconnect:
            logger.warning("WebSocket disconnected. Reconnecting...")
            manager_ws = None
        
        except Exception as e:
            logger.error(f"Error sending heartbeat: {str(e)}")
            manager_ws = None
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)


def get_memory_usage():
    """Get current memory usage (simple mock for demonstration)"""
    
    return {"used_mb": 100, "total_mb": 1024}


def get_cpu_usage():
    """Get current CPU usage (simple mock for demonstration)"""
    
    return {"percent": 15.5}


@app.get("/")
async def root():
    """Root endpoint for health checks"""
    return {
        "worker_id": WORKER_ID,
        "worker_name": WORKER_NAME,
        "status": "running",
        "heartbeats_sent": heartbeat_count,
        "uptime": "sample_uptime"  # Would be actual uptime in production
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "heartbeats_sent": heartbeat_count}



def handle_sigterm(signum, frame):
    logger.info("Received SIGTERM signal, shutting down...")
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=WORKER_PORT)