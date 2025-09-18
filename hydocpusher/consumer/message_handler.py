"""
消息处理器类
负责验证、解析接收到的Pulsar消息，并调用数据转换器进行处理
"""

import asyncio
import json
import logging
from typing import Optional, Dict, Any, Callable, Awaitable
from datetime import datetime

from ..models.message_models import SourceMessageSchema
from ..transformer.data_transformer import DataTransformer
from ..config.settings import AppConfig
from ..config.classification_config import ClassificationConfig, get_classification_config
from ..exceptions.custom_exceptions import (
    ValidationException, MessageProcessException, DataTransformException, ChannelFilteredException
)

logger = logging.getLogger(__name__)


class MessageHandler:
    """消息处理器类"""
    
    def __init__(
        self,
        config: AppConfig,
        data_transformer: Optional[DataTransformer] = None,
        classification_config: Optional[ClassificationConfig] = None
    ):
        """
        初始化消息处理器
        
        Args:
            config: 应用配置
            data_transformer: 数据转换器实例
            classification_config: 分类配置实例
        """
        self.config = config
        self.data_transformer = data_transformer or DataTransformer()
        self.classification_config = classification_config or get_classification_config()
        self._processing_stats = {
            "processed": 0,
            "failed": 0,
            "retried": 0,
            "filtered": 0  # 添加过滤统计
        }
        
    async def handle_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理消息
        
        Args:
            message_data: 原始消息数据
            
        Returns:
            处理结果
            
        Raises:
            MessageProcessException: 消息处理失败时抛出异常
        """
        start_time = datetime.now()
        message_id = None
        
        try:
            logger.info(f"Starting message processing at {start_time}")
            
            # 1. 验证消息格式
            validated_message = await self._validate_message(message_data)
            message_id = validated_message.document_id
            
            logger.info(f"Processing message: {message_id}")
            
            # 2. 检查消息是否可以处理
            validated_message.validate_for_processing()
            
            # 3. 检查频道ID是否在允许列表中
            await self._validate_channel_id(validated_message.channel_id)
            
            # 4. 数据转换
            archive_data = await self._transform_data(validated_message)
            
            # 5. 记录成功统计
            self._processing_stats["processed"] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Message processed successfully: {message_id}, time: {processing_time:.2f}s")
            
            return {
                "success": True,
                "message_id": message_id,
                "processing_time": processing_time,
                "archive_data": archive_data
            }
            
        except ValidationException as e:
            # 验证异常，不重试
            self._processing_stats["failed"] += 1
            logger.error(f"Message validation failed: {message_id}, error: {str(e)}")
            raise MessageProcessException(f"Message validation failed: {str(e)}", cause=e)
            
        except ChannelFilteredException as e:
            # 频道过滤异常，记录并跳过处理
            self._processing_stats["filtered"] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Message filtered due to channel restriction: {message_id}, channel: {e.channel_id}, time: {processing_time:.2f}s")
            
            return {
                "success": True,
                "filtered": True,
                "message_id": message_id,
                "channel_id": e.channel_id,
                "processing_time": processing_time,
                "reason": "Channel not in allowed list"
            }
            
        except DataTransformException as e:
            # 数据转换异常，可以重试
            self._processing_stats["failed"] += 1
            logger.error(f"Data transformation failed: {message_id}, error: {str(e)}")
            raise MessageProcessException(f"Data transformation failed: {str(e)}", cause=e)
            
        except Exception as e:
            # 其他未预期的异常
            self._processing_stats["failed"] += 1
            logger.error(f"Unexpected error processing message: {message_id}, error: {str(e)}")
            raise MessageProcessException(f"Unexpected error processing message: {str(e)}", cause=e)
    
    async def _validate_message(self, message_data: Dict[str, Any]) -> SourceMessageSchema:
        """
        验证消息格式
        
        Args:
            message_data: 原始消息数据
            
        Returns:
            验证后的消息模型
            
        Raises:
            ValidationException: 验证失败时抛出异常
        """
        try:
            logger.debug("Validating message format")
            
            # 检查必需字段
            required_fields = ["MSG", "DATA", "ISSUCCESS"]
            missing_fields = []
            
            for field in required_fields:
                if field not in message_data:
                    missing_fields.append(field)
            
            if missing_fields:
                raise ValidationException(f"Missing required fields: {', '.join(missing_fields)}")
            
            # 预处理消息数据，处理字符串JSON字段
            preprocessed_data = self._preprocess_message_data(message_data)
            
            # 创建消息模型
            message_schema = SourceMessageSchema(**preprocessed_data)
            
            logger.debug(f"Message validation successful: {message_schema.document_id}")
            return message_schema
            
        except ValidationException:
            raise
        except Exception as e:
            raise ValidationException(f"Message validation failed: {str(e)}", cause=e)
    
    def _preprocess_message_data(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        预处理消息数据，处理字符串JSON字段
        
        Args:
            message_data: 原始消息数据
            
        Returns:
            预处理后的消息数据
        """
        try:
            # 深拷贝数据以避免修改原始数据
            import copy
            processed_data = copy.deepcopy(message_data)
            
            # 处理 DATA.DATA.DEFAULTRELDOCS 字段
            if ('DATA' in processed_data and 
                'DATA' in processed_data['DATA'] and 
                'DEFAULTRELDOCS' in processed_data['DATA']['DATA']):
                
                defaultreldocs = processed_data['DATA']['DATA']['DEFAULTRELDOCS']
                
                # 如果是字符串，尝试解析为JSON
                if isinstance(defaultreldocs, str) and defaultreldocs.strip():
                    try:
                        parsed = json.loads(defaultreldocs)
                        # 提取DATA数组，如果解析失败则使用空列表
                        if isinstance(parsed, dict) and 'DATA' in parsed:
                            processed_data['DATA']['DATA']['DEFAULTRELDOCS'] = parsed['DATA']
                        else:
                            processed_data['DATA']['DATA']['DEFAULTRELDOCS'] = []
                        logger.debug("Successfully preprocessed DEFAULTRELDOCS field")
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse DEFAULTRELDOCS JSON: {defaultreldocs}")
                        processed_data['DATA']['DATA']['DEFAULTRELDOCS'] = []
            
            # 处理其他可能的字符串JSON字段
            json_string_fields = [
                'DEFAULTRELDOCS_IRS', 'DOCCOVERPIC', 'LISTPICS', 
                'DOCUMENT_RELATED_APPENDIX', 'DOCUMENT_CONTENT_APPENDIX',
                'DOCUMENT_RELATED_VIDEO', 'DOCUMENT_CONTENT_PIC', 
                'DOCUMENT_CONTENT_VIDEO', 'FOCUSIMAGE', 'DOCUMENT_RELATED_PIC'
            ]
            
            if ('DATA' in processed_data and 'DATA' in processed_data['DATA']):
                data_section = processed_data['DATA']['DATA']
                for field in json_string_fields:
                    if field in data_section and isinstance(data_section[field], str):
                        if data_section[field].strip() in ['[]', '{}', '']:
                            continue  # 保持空字符串不变
                        try:
                            parsed = json.loads(data_section[field])
                            data_section[field] = json.dumps(parsed, ensure_ascii=False)
                        except json.JSONDecodeError:
                            pass  # 保持原值不变
            
            logger.debug("Message data preprocessing completed")
            return processed_data
            
        except Exception as e:
            logger.warning(f"Message data preprocessing failed: {str(e)}")
            return message_data  # 返回原始数据
    
    async def _transform_data(self, message: SourceMessageSchema) -> Dict[str, Any]:
        """
        转换数据
        
        Args:
            message: 验证后的消息
            
        Returns:
            转换后的档案数据
            
        Raises:
            DataTransformException: 转换失败时抛出异常
        """
        try:
            logger.debug(f"Transforming data for message: {message.document_id}")
            
            # 使用数据转换器进行转换
            archive_data = self.data_transformer.transform_message(message)
            
            logger.debug(f"Data transformation successful: {message.document_id}")
            return archive_data
            
        except DataTransformException:
            raise
        except Exception as e:
            raise DataTransformException(f"Data transformation failed: {str(e)}", cause=e)
    
    async def _validate_channel_id(self, channel_id: str) -> None:
        """
        验证频道ID是否在允许的列表中
        
        Args:
            channel_id: 频道ID
            
        Raises:
            ChannelFilteredException: 频道ID不在允许列表中时抛出异常
        """
        try:
            logger.debug(f"Validating channel ID: {channel_id}")
            
            # 获取所有允许的频道ID
            allowed_channels = self.classification_config.get_channel_ids()
            
            if str(channel_id) not in allowed_channels:
                logger.debug(f"Channel ID {channel_id} not in allowed list: {allowed_channels}")
                raise ChannelFilteredException(
                    f"Channel ID {channel_id} is not in the allowed channel list", 
                    channel_id=channel_id
                )
            
            logger.debug(f"Channel ID {channel_id} validation successful")
            
        except ChannelFilteredException:
            raise
        except Exception as e:
            logger.warning(f"Error validating channel ID {channel_id}: {str(e)}")
            # 如果验证出错，允许继续处理（保守策略）
            pass
    
    def set_data_transformer(self, transformer: DataTransformer) -> None:
        """
        设置数据转换器
        
        Args:
            transformer: 数据转换器实例
        """
        self.data_transformer = transformer
        logger.info("Data transformer updated")
    
    def set_classification_config(self, config: ClassificationConfig) -> None:
        """
        设置分类配置
        
        Args:
            config: 分类配置实例
        """
        self.classification_config = config
        logger.info("Classification config updated")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            处理统计信息
        """
        return self._processing_stats.copy()
    
    def reset_stats(self) -> None:
        """
        重置处理统计信息
        """
        self._processing_stats = {
            "processed": 0,
            "failed": 0,
            "retried": 0,
            "filtered": 0
        }
        logger.info("Processing stats reset")


class MessageProcessor:
    """消息处理器（兼容旧版本）"""
    
    def __init__(self, config: AppConfig):
        """
        初始化消息处理器
        
        Args:
            config: 应用配置
        """
        self.config = config
        self.handler = MessageHandler(config)
        
    async def process_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理消息（兼容接口）
        
        Args:
            message_data: 消息数据
            
        Returns:
            处理结果
        """
        return await self.handler.handle_message(message_data)
    
    @property
    def stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息
        """
        return self.handler.get_processing_stats()