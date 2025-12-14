"""
Skill基类模块
定义所有数据处理技能的基础接口和通用功能
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from loguru import logger
import time

from ..models import DataChunk, ProcessMetadata, ProcessingStatus, ProcessingResult
from ..config import config_manager


class BaseSkill(ABC):
    """Skill基类，定义通用接口和方法"""
    
    def __init__(self, name: str, config_key: str):
        """
        初始化Skill
        
        Args:
            name: Skill名称
            config_key: 配置文件中的键
        """
        self.name = name
        self.config_key = config_key
        self.config = config_manager.get(config_key, {})
        self.global_config = config_manager.get('global', {})
        
        # 从配置中获取参数
        self.max_workers = self.global_config.get('max_workers', 4)
        self.output_dir = self.global_config.get('output_dir', './output')
        self.temp_dir = self.global_config.get('temp_dir', './temp')
        
        logger.info(f"{self.name} 初始化完成")
    
    @abstractmethod
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """
        处理单个数据块（抽象方法）
        
        Args:
            data_chunk: 输入数据块
            
        Returns:
            处理后的数据块
        """
        pass
    
    def process_batch(self, data_chunks: List[DataChunk]) -> List[DataChunk]:
        """
        批量处理数据块
        
        Args:
            data_chunks: 输入数据块列表
            
        Returns:
            处理后的数据块列表
        """
        logger.info(f"开始批量处理 {len(data_chunks)} 个数据块")
        start_time = time.time()
        
        processed_chunks = []
        
        for i, chunk in enumerate(data_chunks):
            try:
                # 创建处理元信息
                process_start = datetime.now()
                
                # 处理单个数据块
                processed_chunk = self.process_single(chunk)
                
                # 计算处理时间
                process_end = datetime.now()
                processing_time = (process_end - process_start).total_seconds()
                
                # 添加处理元信息
                process_metadata = ProcessMetadata(
                    processor_name=self.name,
                    processing_time=processing_time,
                    start_time=process_start,
                    end_time=process_end,
                    status=ProcessingStatus.COMPLETED,
                    parameters=self.config
                )
                
                processed_chunk.add_process_metadata(process_metadata)
                processed_chunks.append(processed_chunk)
                
                # 记录进度
                if (i + 1) % 10 == 0:
                    logger.info(f"已处理 {i + 1}/{len(data_chunks)} 个数据块")
                
            except Exception as e:
                logger.error(f"处理数据块 {chunk.id} 时出错: {e}")
                
                # 创建失败的处理元信息
                process_end = datetime.now()
                error_metadata = ProcessMetadata(
                    processor_name=self.name,
                    processing_time=(process_end - datetime.now()).total_seconds(),
                    start_time=datetime.now(),
                    end_time=process_end,
                    status=ProcessingStatus.FAILED,
                    error_message=str(e),
                    parameters=self.config
                )
                
                chunk.add_process_metadata(error_metadata)
                processed_chunks.append(chunk)
        
        total_time = time.time() - start_time
        success_count = sum(1 for chunk in processed_chunks 
                          if chunk.get_latest_status() == ProcessingStatus.COMPLETED)
        
        logger.info(f"批量处理完成: {success_count}/{len(data_chunks)} 成功, 耗时 {total_time:.2f}s")
        
        return processed_chunks
    
    def validate_input(self, data_chunk: DataChunk) -> bool:
        """
        验证输入数据
        
        Args:
            data_chunk: 输入数据块
            
        Returns:
            是否有效
        """
        if not data_chunk.content.strip():
            logger.warning(f"数据块 {data_chunk.id} 内容为空")
            return False
        
        return True
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键
            default: 默认值
            
        Returns:
            配置值
        """
        return self.config.get(key, default)
    
    def log_processing_info(self, message: str, level: str = "info"):
        """
        记录处理信息
        
        Args:
            message: 日志消息
            level: 日志级别
        """
        log_message = f"[{self.name}] {message}"
        
        if level.lower() == "debug":
            logger.debug(log_message)
        elif level.lower() == "info":
            logger.info(log_message)
        elif level.lower() == "warning":
            logger.warning(log_message)
        elif level.lower() == "error":
            logger.error(log_message)
        else:
            logger.info(log_message)
    
    def get_skill_statistics(self) -> Dict[str, Any]:
        """
        获取技能统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "name": self.name,
            "config_key": self.config_key,
            "config": self.config,
            "max_workers": self.max_workers
        }


class ConfigurableSkill(BaseSkill):
    """可配置的Skill基类"""
    
    def __init__(self, name: str, config_key: str):
        super().__init__(name, config_key)
        self.validate_config()
    
    def validate_config(self):
        """验证配置（子类可重写）"""
        required_keys = self.get_required_config_keys()
        
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"{self.name} 缺少必需的配置项: {key}")
    
    def get_required_config_keys(self) -> List[str]:
        """
        获取必需的配置键列表（子类可重写）
        
        Returns:
            必需配置键列表
        """
        return []


class MonitoredSkill(ConfigurableSkill):
    """带监控功能的Skill基类"""
    
    def __init__(self, name: str, config_key: str):
        super().__init__(name, config_key)
        self.processed_count = 0
        self.failed_count = 0
        self.total_processing_time = 0.0
    
    def process_batch(self, data_chunks: List[DataChunk]) -> List[DataChunk]:
        """重写批量处理方法，添加监控功能"""
        start_time = time.time()
        
        # 调用父类方法
        processed_chunks = super().process_batch(data_chunks)
        
        # 更新统计信息
        self.processed_count += len(data_chunks)
        self.failed_count += sum(1 for chunk in processed_chunks 
                               if chunk.get_latest_status() == ProcessingStatus.FAILED)
        self.total_processing_time += time.time() - start_time
        
        return processed_chunks
    
    def get_monitoring_info(self) -> Dict[str, Any]:
        """
        获取监控信息
        
        Returns:
            监控信息字典
        """
        success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
        avg_processing_time = self.total_processing_time / self.processed_count if self.processed_count > 0 else 0
        
        return {
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "success_rate": success_rate,
            "average_processing_time": avg_processing_time,
            "total_processing_time": self.total_processing_time
        }
    
    def reset_statistics(self):
        """重置统计信息"""
        self.processed_count = 0
        self.failed_count = 0
        self.total_processing_time = 0.0