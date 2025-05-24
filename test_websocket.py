#!/usr/bin/env python3

import asyncio
import json
import logging
import websockets
import aiohttp
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_manager_connection():
    """Test basic connection to manager"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://manager:8000/health") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Manager health check: {data}")
                    return True
                else:
                    logger.error(f"Manager health check failed: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Failed to connect to manager: {e}")
        return False

async def test_worker_registration():
    """Test worker registration"""
    registration_data = {
        "worker_id": "test-worker",
        "worker_name": "Test Worker",
        "host": "test",
        "port": 9999,
        "description": "Test worker for debugging"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("http://manager:8000/register", json=registration_data) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Registration successful: {data}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Registration failed: {response.status} - {error_text}")
                    return False
    except Exception as e:
        logger.error(f"Failed to register: {e}")
        return False

async def test_websocket_connection():
    """Test WebSocket connection and heartbeat"""
    worker_id = "test-worker"
    websocket_url = f"ws://manager:8000/heartbeat/{worker_id}"
    
    try:
        logger.info(f"Connecting to WebSocket: {websocket_url}")
        
        async with websockets.connect(websocket_url) as websocket:
            logger.info("WebSocket connected successfully!")
            
            # Send a test heartbeat
            heartbeat_message = {
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat(),
                "worker_id": worker_id,
                "worker_name": "Test Worker",
                "health": {
                    "status": "healthy",
                    "cpu_percent": 25.0,
                    "memory_percent": 45.0
                }
            }
            
            await websocket.send(json.dumps(heartbeat_message))
            logger.info("Heartbeat sent!")
            
            # Wait for acknowledgment
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                ack_data = json.loads(response)
                logger.info(f"Received acknowledgment: {ack_data}")
            except asyncio.TimeoutError:
                logger.warning("No acknowledgment received within timeout")
            
            return True
            
    except Exception as e:
        logger.error(f"WebSocket connection failed: {e}")
        return False

async def cleanup_test_worker():
    """Clean up test worker"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete("http://manager:8000/unregister/test-worker") as response:
                if response.status == 200:
                    logger.info("Test worker unregistered successfully")
                else:
                    logger.warning(f"Failed to unregister test worker: {response.status}")
    except Exception as e:
        logger.error(f"Failed to cleanup: {e}")

async def main():
    """Main test function"""
    logger.info("Starting WebSocket connection test...")
    
    # Test 1: Manager health check
    logger.info("=== Test 1: Manager Health Check ===")
    if not await test_manager_connection():
        logger.error("Manager health check failed, aborting tests")
        return
    
    # Test 2: Worker registration
    logger.info("=== Test 2: Worker Registration ===")
    if not await test_worker_registration():
        logger.error("Worker registration failed, aborting tests")
        return
    
    # Wait a moment for registration to be processed
    await asyncio.sleep(2)
    
    # Test 3: WebSocket connection and heartbeat
    logger.info("=== Test 3: WebSocket Connection ===")
    if await test_websocket_connection():
        logger.info("WebSocket test passed!")
    else:
        logger.error("WebSocket test failed!")
    
    # Cleanup
    logger.info("=== Cleanup ===")
    await cleanup_test_worker()
    
    logger.info("Test completed!")

if __name__ == "__main__":
    asyncio.run(main())