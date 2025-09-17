"""
HyDocPusher - 档案推送服务
一个高效的异步数据同步服务，用于将内容发布系统的消息自动转换为档案系统格式并推送到第三方电子档案管理系统
"""

__version__ = "1.0.0"
__author__ = "HyDocPusher Team"
__description__ = "档案推送服务 - 异步消息处理和数据转换"

# 导出主要组件
from .config.settings import get_config
from .consumer.pulsar_consumer import PulsarConsumer
from .consumer.message_handler import MessageHandler
from .transformer.data_transformer import DataTransformer
from .client.archive_client import ArchiveClient
from .services.health_service import get_health_service

__all__ = [
    "get_config",
    "PulsarConsumer", 
    "MessageHandler",
    "DataTransformer",
    "ArchiveClient",
    "get_health_service",
    "__version__",
    "__author__",
    "__description__",
]
