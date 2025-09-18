#!/usr/bin/env python3
"""
HyDocPusher FastAPI 应用入口
集成 Web API 和消息处理功能
"""

import asyncio
import logging
import sys
import os
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

# 确保logs目录存在
os.makedirs('logs', exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/hydocpusher.log', encoding='utf-8')
    ]
)

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

from hydocpusher.config.settings import get_config
from hydocpusher.consumer.pulsar_consumer import PulsarConsumer
from hydocpusher.consumer.message_handler import MessageHandler
from hydocpusher.transformer.data_transformer import DataTransformer
from hydocpusher.client.archive_client import ArchiveClient
from hydocpusher.services.health_service import get_health_service, HealthService
from hydocpusher.services.health_server import start_health_server
from hydocpusher.exceptions.custom_exceptions import (
    ConnectionException,
    ConfigurationException,
    MessageProcessException
)

logger = logging.getLogger(__name__)

# 全局组件
config = get_config()
health_service: Optional[HealthService] = None
health_server = None
pulsar_consumer: Optional[PulsarConsumer] = None
archive_client: Optional[ArchiveClient] = None
message_handler: Optional[MessageHandler] = None
data_transformer: Optional[DataTransformer] = None

async def initialize_components():
    """初始化所有组件"""
    global health_service, health_server, pulsar_consumer, archive_client, message_handler, data_transformer
    
    try:
        logger.info("Initializing HyDocPusher components...")
        
        # 验证配置
        config.validate_required_configs()
        
        # 初始化数据转换器
        data_transformer = DataTransformer()
        
        # 初始化档案客户端
        archive_client = ArchiveClient()
        
        # 初始化消息处理器
        message_handler = MessageHandler(
            config=config,
            data_transformer=data_transformer
        )
        
        # 创建消息处理回调
        async def process_complete_message(message_data: Dict[str, Any]) -> None:
            """完整的消息处理流程"""
            message_id = None
            try:
                # 处理和转换消息
                result = await message_handler.handle_message(message_data)
                
                if result.get("success"):
                    message_id = result.get("message_id")
                    
                    # 检查是否是被过滤的消息
                    if result.get("filtered"):
                        logger.info(f"Message {message_id} filtered, skipping archive processing.")
                        return
                    
                    # 只有未被过滤的消息才发送到档案系统
                    archive_data = result.get("archive_data")
                    
                    logger.info(f"Message {message_id} processed, sending to archive...")
                    
                    # 发送到档案系统
                    archive_response = await archive_client.send_archive_data(archive_data)
                    logger.info(f"Message {message_id} archived: {archive_response}")
                else:
                    logger.error(f"Message processing failed: {result}")
                    
            except Exception as e:
                logger.error(f"Message processing failed for {message_id}: {str(e)}")
                raise
        
        # 初始化Pulsar消费者
        pulsar_consumer = PulsarConsumer(
            config=config,
            message_handler=process_complete_message
        )
        
        # 初始化健康检查服务
        health_service = get_health_service()
        health_service.set_components(
            archive_client=archive_client,
            pulsar_consumer=pulsar_consumer
        )
        
        # 启动独立健康检查服务器 (8081端口)
        health_server = await start_health_server(
            health_service=health_service,
            host="0.0.0.0",
            port=8081
        )
        logger.info("Health server started on port 8081")
        
        logger.info("All components initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize components: {str(e)}")
        raise


async def start_message_processing():
    """启动消息处理"""
    try:
        if pulsar_consumer:
            await pulsar_consumer.connect()
            # 在后台启动消息消费
            asyncio.create_task(pulsar_consumer.start_consuming())
            logger.info("Message processing started")
    except Exception as e:
        logger.error(f"Failed to start message processing: {str(e)}")
        raise


async def cleanup_components():
    """清理所有组件"""
    logger.info("Cleaning up components...")
    
    if pulsar_consumer:
        await pulsar_consumer.close()
    
    if archive_client:
        await archive_client.close_async()
    
    # 关闭健康检查服务器
    if health_server:
        await health_server.stop()
        logger.info("Health server stopped")
    
    logger.info("Components cleanup completed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    await initialize_components()
    await start_message_processing()
    yield
    # 关闭时
    await cleanup_components()


# 创建 FastAPI 应用
app = FastAPI(
    title=config.app_name,
    version=config.app_version,
    description="HyDocPusher - 档案推送服务",
    lifespan=lifespan
)


@app.get("/")
async def root():
    """根路径"""
    return {
        "service": config.app_name,
        "version": config.app_version,
        "message": "HyDocPusher API is running",
        "endpoints": {
            "health": "/health",
            "detailed_health": "/health/detailed",
            "info": "/info"
        }
    }


@app.get("/health")
async def health_check():
    """基本健康检查"""
    try:
        if not health_service:
            raise HTTPException(status_code=503, detail="Health service not initialized")
        
        health = await health_service.check_system_health()
        
        status_code = 200
        if health.status.value == 'unhealthy':
            status_code = 503
        
        return JSONResponse(
            content={
                'status': health.status.value,
                'timestamp': health.timestamp.isoformat(),
                'uptime_seconds': health.uptime_seconds,
                'message': f"System is {health.status.value}"
            },
            status_code=status_code
        )
        
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@app.get("/health/detailed")
async def detailed_health_check():
    """详细健康检查"""
    try:
        if not health_service:
            raise HTTPException(status_code=503, detail="Health service not initialized")
        
        health = await health_service.check_system_health()
        response_data = health_service.to_dict(health)
        
        status_code = 200
        if health.status.value == 'unhealthy':
            status_code = 503
        
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        logger.error(f"Detailed health check error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Detailed health check failed: {str(e)}")


@app.get("/info")
async def get_info():
    """获取应用信息"""
    return {
        'name': config.app_name,
        'version': config.app_version,
        'description': 'HyDocPusher - 档案推送服务',
        'config': {
            'pulsar_topic': config.pulsar.topic,
            'archive_url': config.archive.api_url,
            'debug_mode': config.debug
        }
    }


if __name__ == "__main__":
    # 直接运行时使用 uvicorn
    uvicorn.run(
        app,
        host=config.server_host,
        port=config.server_port,
        reload=config.debug,
        log_level="info"
    )
