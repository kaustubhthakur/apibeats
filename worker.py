import asyncio
import json
import logging
import os
import psutil
import signal
import sys
from datetime import datetime
from typing import Optional
import websockets
import aiohttp
from fastapi import FastAPI
import uvicorn
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeartbeatWorker:
    def __init__(
        self, 
        worker_name: str, 
        manager_host: str = "manager",
        manager_port: int = 8000,
        heartbeat_interval: int = 10
    ):
        self.worker_id = str(uuid.uuid4())
        self.worker_name = worker_name
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.heartbeat_interval = heartbeat_interval
        self.websocket = None
        self.is_running = False
        self.app_host = "0.0.0.0"
        self.app_port = int(os.getenv("WORKER_PORT", "8001"))
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # FastAPI app for the worker
        self.app = FastAPI(title=f"Worker: {worker_name}", version="1.0.0")
        self.setup_routes()
        
        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_routes(self):
        """Setup FastAPI routes for the worker"""
        
        @self.app.get("/")
        async def root():
            return {
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
                "status": "running",
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.get("/health")
        async def health():
            """Worker health endpoint"""
            return await self.get_health_data()
        
        @self.app.get("/info")
        async def info():
            """Worker information"""
            return {
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
                "manager_host": self.manager_host,
                "manager_port": self.manager_port,
                "heartbeat_interval": self.heartbeat_interval,
                "uptime": datetime.now().isoformat(),
                "is_connected": self.websocket is not None and not self.websocket.closed,
                "reconnect_attempts": self.reconnect_attempts
            }
    
    async def get_health_data(self):
        """Get worker health metrics"""
        try:
            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "timestamp": datetime.now().isoformat(),
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
                "system": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_used": memory.used,
                    "memory_total": memory.total,
                    "disk_percent": disk.percent,
                    "disk_used": disk.used,
                    "disk_total": disk.total
                },
                "process": {
                    "pid": os.getpid(),
                    "threads": psutil.Process().num_threads(),
                    "memory_info": psutil.Process().memory_info()._asdict()
                }
            }
        except Exception as e:
            logger.error(f"Error getting health data: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
                "error": str(e)
            }
    
    async def register_with_manager(self):
        """Register this worker with the manager"""
        registration_data = {
            "worker_id": self.worker_id,
            "worker_name": self.worker_name,
            "host": self.app_host,
            "port": self.app_port,
            "description": f"Worker {self.worker_name} running on port {self.app_port}"
        }
        
        manager_url = f"http://{self.manager_host}:{self.manager_port}/register"
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(manager_url, json=registration_data, timeout=10) as response:
                        if response.status == 200:
                            result = await response.json()
                            logger.info(f"Successfully registered with manager: {result}")
                            return True
                        else:
                            error_text = await response.text()
                            logger.error(f"Failed to register with manager: {response.status} - {error_text}")
            except Exception as e:
                logger.error(f"Error registering with manager (attempt {attempt + 1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5)  # Wait before retry
        
        return False
    
    async def connect_websocket(self):
        """Connect to manager via WebSocket for heartbeats"""
        # Updated to match the manager's WebSocket endpoint path
        websocket_url = f"ws://{self.manager_host}:{self.manager_port}/ws/heartbeat/{self.worker_id}"
        
        try:
            logger.info(f"Connecting to WebSocket at {websocket_url}")
            self.websocket = await websockets.connect(
                websocket_url,
                timeout=10,
                ping_interval=20,
                ping_timeout=10
            )
            logger.info(f"Connected to manager WebSocket at {websocket_url}")
            self.reconnect_attempts = 0  # Reset counter on successful connection
            return True
        except Exception as e:
            logger.error(f"Failed to connect to manager WebSocket: {e}")
            self.reconnect_attempts += 1
            return False
    
    async def send_heartbeat(self):
        """Send heartbeat to manager"""
        if not self.websocket or self.websocket.closed:
            return False
        
        try:
            health_data = await self.get_health_data()
            heartbeat_message = {
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat(),
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
                "health": health_data
            }
            
            await self.websocket.send(json.dumps(heartbeat_message))
            logger.debug(f"Sent heartbeat to manager")
            
            # Wait for acknowledgment (with timeout)
            try:
                response = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
                ack_data = json.loads(response)
                if ack_data.get("type") == "heartbeat_ack":
                    logger.debug(f"Heartbeat acknowledged by manager")
                    return True
                else:
                    logger.warning(f"Unexpected response from manager: {ack_data}")
                    return False
            except asyncio.TimeoutError:
                logger.warning("Heartbeat acknowledgment timeout")
                return False
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from manager: {e}")
                return False
                
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed during heartbeat")
            return False
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
            return False
    
    async def heartbeat_loop(self):
        """Main heartbeat loop"""
        while self.is_running:
            try:
                # Check WebSocket connection
                if not self.websocket or self.websocket.closed:
                    logger.info("WebSocket connection lost, attempting to reconnect...")
                    
                    if self.reconnect_attempts >= self.max_reconnect_attempts:
                        logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached")
                        await asyncio.sleep(30)  # Wait longer before trying again
                        self.reconnect_attempts = 0  # Reset counter
                        continue
                    
                    if not await self.connect_websocket():
                        logger.error("Failed to reconnect to manager")
                        await asyncio.sleep(min(self.heartbeat_interval * (self.reconnect_attempts + 1), 60))
                        continue
                
                # Send heartbeat
                if not await self.send_heartbeat():
                    logger.warning("Failed to send heartbeat")
                
                # Wait before next heartbeat
                await asyncio.sleep(self.heartbeat_interval)
                
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(self.heartbeat_interval)
    
    async def unregister_from_manager(self):
        """Unregister this worker from the manager"""
        unregister_url = f"http://{self.manager_host}:{self.manager_port}/unregister/{self.worker_id}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(unregister_url, timeout=10) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"Successfully unregistered from manager: {result}")
                    else:
                        logger.error(f"Failed to unregister from manager: {response.status}")
        except Exception as e:
            logger.error(f"Error unregistering from manager: {e}")
    
    async def start(self):
        """Start the worker"""
        logger.info(f"Starting worker {self.worker_name} (ID: {self.worker_id})")
        
        # Register with manager
        if not await self.register_with_manager():
            logger.error("Failed to register with manager, exiting...")
            return
        
        # Connect WebSocket
        if not await self.connect_websocket():
            logger.error("Failed to connect WebSocket initially, will retry in heartbeat loop...")
        
        self.is_running = True
        
        # Start heartbeat loop in background
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        
        # Start FastAPI server
        config = uvicorn.Config(
            self.app, 
            host=self.app_host, 
            port=self.app_port,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        try:
            # Run both the FastAPI server and heartbeat loop
            await asyncio.gather(
                server.serve(),
                heartbeat_task,
                return_exceptions=True
            )
        except Exception as e:
            logger.error(f"Error running worker: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the worker gracefully"""
        logger.info(f"Stopping worker {self.worker_name}")
        self.is_running = False
        
        # Close WebSocket connection
        if self.websocket and not self.websocket.closed:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        
        # Unregister from manager
        await self.unregister_from_manager()
        
        logger.info(f"Worker {self.worker_name} stopped")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.is_running = False

async def main():
    """Main function to run the worker"""
    worker_name = os.getenv("WORKER_NAME", f"worker-{uuid.uuid4().hex[:8]}")
    manager_host = os.getenv("MANAGER_HOST", "manager")
    manager_port = int(os.getenv("MANAGER_PORT", "8000"))
    heartbeat_interval = int(os.getenv("HEARTBEAT_INTERVAL", "10"))
    
    worker = HeartbeatWorker(
        worker_name=worker_name,
        manager_host=manager_host,
        manager_port=manager_port,
        heartbeat_interval=heartbeat_interval
    )
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Worker failed: {e}")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())