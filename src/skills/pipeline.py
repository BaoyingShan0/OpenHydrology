"""
水利数据流程控制器
负责协调和执行整个数据处理流程，整合所有技能组件
"""

import os
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from loguru import logger

try:
    from dataflow import DataFlow
    DATAFLOW_SUPPORT = True
except ImportError:
    DATAFLOW_SUPPORT = False
    logger.warning("DataFlow库未安装，将使用内置流程控制器")

from .base import BaseSkill
from .parser import HydroDataParser
from .cleaner import HydroDataCleaner
from .enhancer import HydroDataEnhancer
from .evaluator import HydroDataEvaluator
from ..models import (
    DataChunk, ProcessedData, ProcessingResult, PipelineConfig,
    ProcessingStatus, QAData
)
from ..config import config_manager
from ..utils import (
    save_json, load_json, get_timestamp, create_directories,
    format_file_size, calculate_file_hash
)


class HydroDataPipeline(BaseSkill):
    """
    水利数据流程控制器
    
    功能：
    1. 协调所有数据处理技能的执行顺序
    2. 支持批量和增量数据处理
    3. 提供检查点和错误恢复机制
    4. 支持并行和分布式处理
    5. 生成处理报告和统计信息
    6. 集成DataFlow工作流引擎
    """
    
    def __init__(self, config_key: str = "pipeline"):
        super().__init__("HydroDataPipeline", config_key)
        
        # 初始化配置
        self.pipeline_config = PipelineConfig()
        self._load_pipeline_config()
        
        # 初始化技能组件
        self.skills = {}
        self._init_skills()
        
        # 处理统计
        self.processing_stats = {
            "total_files": 0,
            "processed_files": 0,
            "failed_files": 0,
            "total_chunks": 0,
            "processed_chunks": 0,
            "failed_chunks": 0,
            "start_time": None,
            "end_time": None,
            "total_time": 0.0
        }
        
        # 检查点管理
        self.checkpoint_dir = Path(self.temp_dir) / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # DataFlow集成
        self.dataflow = None
        if DATAFLOW_SUPPORT:
            self._init_dataflow()
        
        self.log_processing_info("数据流程控制器初始化完成")
    
    def get_required_config_keys(self) -> List[str]:
        """获取必需的配置键"""
        return ["batch_size", "max_workers"]
    
    def _load_pipeline_config(self):
        """加载流水线配置"""
        self.pipeline_config.batch_size = self.get_config_value("batch_size", 100)
        self.pipeline_config.max_workers = self.get_config_value("max_workers", 4)
        self.pipeline_config.checkpoint_enabled = self.get_config_value("checkpoint_enabled", True)
        self.pipeline_config.error_handling = self.get_config_value("error_handling", "skip")
        
        # 技能启用状态
        self.pipeline_config.parser_enabled = True  # Parser总是启用
        self.pipeline_config.cleaner_enabled = config_manager.get("cleaner", {}).get("remove_duplicates", True)
        self.pipeline_config.enhancer_enabled = config_manager.get("enhancer", {}).get("enable_qa_generation", True)
        self.pipeline_config.evaluator_enabled = config_manager.get("evaluator", {}).get("quality_metrics", [])
    
    def _init_skills(self):
        """初始化技能组件"""
        try:
            # Parser（必需）
            self.skills["parser"] = HydroDataParser()
            
            # Cleaner（可选）
            if self.pipeline_config.cleaner_enabled:
                self.skills["cleaner"] = HydroDataCleaner()
            
            # Enhancer（可选）
            if self.pipeline_config.enhancer_enabled:
                self.skills["enhancer"] = HydroDataEnhancer()
            
            # Evaluator（可选）
            if self.pipeline_config.evaluator_enabled:
                self.skills["evaluator"] = HydroDataEvaluator()
            
            self.log_processing_info(f"已初始化 {len(self.skills)} 个技能组件")
            
        except Exception as e:
            self.log_processing_info(f"技能组件初始化失败: {e}", "error")
            raise
    
    def _init_dataflow(self):
        """初始化DataFlow"""
        try:
            # 创建DataFlow实例（这里需要根据实际的DataFlow API调整）
            self.dataflow = DataFlow()
            self.log_processing_info("DataFlow集成初始化成功")
        except Exception as e:
            self.log_processing_info(f"DataFlow初始化失败: {e}", "warning")
            self.dataflow = None
    
    def process_files(self, file_paths: Union[str, List[str]], 
                     output_path: Optional[str] = None) -> ProcessingResult:
        """
        处理文件列表
        
        Args:
            file_paths: 文件路径或文件路径列表
            output_path: 输出文件路径
            
        Returns:
            处理结果
        """
        start_time = time.time()
        self.processing_stats["start_time"] = datetime.now()
        
        # 标准化文件路径
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        
        self.processing_stats["total_files"] = len(file_paths)
        self.log_processing_info(f"开始处理 {len(file_paths)} 个文件")
        
        try:
            # 1. 解析阶段
            all_chunks = []
            for file_path in file_paths:
                try:
                    chunks = self.skills["parser"].parse_file(file_path)
                    all_chunks.extend(chunks)
                    self.processing_stats["processed_files"] += 1
                    self.log_processing_info(f"文件解析成功: {file_path}")
                except Exception as e:
                    self.processing_stats["failed_files"] += 1
                    self.log_processing_info(f"文件解析失败: {file_path} - {e}", "error")
                    
                    if self.pipeline_config.error_handling == "stop":
                        raise
            
            self.processing_stats["total_chunks"] = len(all_chunks)
            self.log_processing_info(f"解析完成，共生成 {len(all_chunks)} 个数据块")
            
            # 2. 处理阶段
            if all_chunks:
                processed_chunks = self._process_chunks(all_chunks)
            else:
                processed_chunks = []
            
            # 3. 创建处理结果
            processed_data = ProcessedData(
                name=f"processed_data_{get_timestamp()}",
                description=f"处理 {len(file_paths)} 个文件生成的数据",
                chunks=processed_chunks
            )
            
            # 添加问答对
            for chunk in processed_chunks:
                if "generated_qa" in chunk.extra_data:
                    for qa_data in chunk.extra_data["generated_qa"]:
                        processed_data.add_qa_pair(QAData(**qa_data))
            
            # 4. 保存结果
            if output_path:
                self._save_processed_data(processed_data, output_path)
            
            # 5. 更新统计信息
            end_time = time.time()
            self.processing_stats["end_time"] = datetime.now()
            self.processing_stats["total_time"] = end_time - start_time
            self.processing_stats["processed_chunks"] = len(processed_chunks)
            
            self.log_processing_info(f"文件处理完成，耗时 {end_time - start_time:.2f}s")
            
            return ProcessingResult(
                success=True,
                data=processed_data,
                processing_time=end_time - start_time,
                metadata={
                    "stats": self.processing_stats,
                    "file_count": len(file_paths),
                    "chunk_count": len(processed_chunks)
                }
            )
            
        except Exception as e:
            self.log_processing_info(f"文件处理失败: {e}", "error")
            return ProcessingResult(
                success=False,
                error_message=str(e),
                processing_time=time.time() - start_time,
                metadata={"stats": self.processing_stats}
            )
    
    def process_directory(self, directory_path: str, recursive: bool = True,
                        output_path: Optional[str] = None) -> ProcessingResult:
        """
        处理目录下的所有文件
        
        Args:
            directory_path: 目录路径
            recursive: 是否递归处理子目录
            output_path: 输出文件路径
            
        Returns:
            处理结果
        """
        directory = Path(directory_path)
        
        if not directory.exists() or not directory.is_dir():
            return ProcessingResult(
                success=False,
                error_message=f"目录不存在或不是有效目录: {directory_path}"
            )
        
        # 获取所有支持的文件
        parser = self.skills["parser"]
        supported_formats = parser.get_supported_formats()
        
        all_files = []
        if recursive:
            for ext in supported_formats:
                pattern = f"**/*.{ext}"
                files = list(directory.glob(pattern))
                all_files.extend([str(f) for f in files])
        else:
            for ext in supported_formats:
                pattern = f"*.{ext}"
                files = list(directory.glob(pattern))
                all_files.extend([str(f) for f in files])
        
        if not all_files:
            return ProcessingResult(
                success=False,
                error_message=f"目录中未找到支持的文件格式: {directory_path}"
            )
        
        self.log_processing_info(f"在目录中找到 {len(all_files)} 个文件")
        
        return self.process_files(all_files, output_path)
    
    def _process_chunks(self, chunks: List[DataChunk]) -> List[DataChunk]:
        """处理数据块列表"""
        self.log_processing_info(f"开始处理 {len(chunks)} 个数据块")
        
        processed_chunks = chunks.copy()
        
        # 按顺序执行各个技能
        skill_order = ["cleaner", "enhancer", "evaluator"]
        
        for skill_name in skill_order:
            if skill_name in self.skills:
                skill = self.skills[skill_name]
                self.log_processing_info(f"执行 {skill_name} 技能")
                
                # 分批处理
                batch_size = self.pipeline_config.batch_size
                processed_chunks = self._process_batches(skill, processed_chunks, batch_size)
        
        return processed_chunks
    
    def _process_batches(self, skill: BaseSkill, chunks: List[DataChunk], 
                       batch_size: int) -> List[DataChunk]:
        """分批处理数据块"""
        processed_chunks = []
        
        if self.pipeline_config.max_workers > 1 and self.pipeline_config.parallel_processing:
            # 并行处理
            processed_chunks = self._process_parallel(skill, chunks, batch_size)
        else:
            # 串行处理
            for i in range(0, len(chunks), batch_size):
                batch = chunks[i:i + batch_size]
                processed_batch = skill.process_batch(batch)
                processed_chunks.extend(processed_batch)
                
                # 检查点保存
                if self.pipeline_config.checkpoint_enabled and i % (batch_size * 5) == 0:
                    self._save_checkpoint(skill.name, processed_batch, i)
        
        return processed_chunks
    
    def _process_parallel(self, skill: BaseSkill, chunks: List[DataChunk],
                          batch_size: int) -> List[DataChunk]:
        """并行处理数据块"""
        processed_chunks = []
        
        with ThreadPoolExecutor(max_workers=self.pipeline_config.max_workers) as executor:
            # 分批
            batches = [chunks[i:i + batch_size] for i in range(0, len(chunks), batch_size)]
            
            # 并行处理
            futures = [executor.submit(skill.process_batch, batch) for batch in batches]
            
            # 收集结果
            for future in futures:
                try:
                    batch_result = future.result()
                    processed_chunks.extend(batch_result)
                except Exception as e:
                    self.log_processing_info(f"并行处理批次失败: {e}", "error")
                    
                    if self.pipeline_config.error_handling == "stop":
                        raise
        
        return processed_chunks
    
    def _save_checkpoint(self, skill_name: str, chunks: List[DataChunk], index: int):
        """保存检查点"""
        if not self.pipeline_config.checkpoint_enabled:
            return
        
        checkpoint_file = self.checkpoint_dir / f"{skill_name}_{index}_{get_timestamp()}.json"
        checkpoint_data = {
            "skill_name": skill_name,
            "index": index,
            "timestamp": get_timestamp(),
            "chunk_count": len(chunks),
            "chunks": [
                {
                    "id": chunk.id,
                    "content": chunk.content,
                    "extra_data": chunk.extra_data
                }
                for chunk in chunks
            ]
        }
        
        try:
            save_json(checkpoint_data, str(checkpoint_file))
            self.log_processing_info(f"检查点保存成功: {checkpoint_file}")
        except Exception as e:
            self.log_processing_info(f"检查点保存失败: {e}", "error")
    
    def load_checkpoint(self, checkpoint_file: str) -> List[DataChunk]:
        """加载检查点"""
        try:
            checkpoint_data = load_json(checkpoint_file)
            chunks = []
            
            for chunk_data in checkpoint_data.get("chunks", []):
                chunk = DataChunk(
                    id=chunk_data["id"],
                    content=chunk_data["content"],
                    extra_data=chunk_data.get("extra_data", {})
                )
                chunks.append(chunk)
            
            self.log_processing_info(f"检查点加载成功: {checkpoint_file}")
            return chunks
            
        except Exception as e:
            self.log_processing_info(f"检查点加载失败: {e}", "error")
            return []
    
    def _save_processed_data(self, data: ProcessedData, output_path: str):
        """保存处理后的数据"""
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 准备保存数据
        save_data = {
            "id": data.id,
            "name": data.name,
            "description": data.description,
            "created_at": data.created_at.isoformat(),
            "updated_at": data.updated_at.isoformat(),
            "statistics": data.get_statistics(),
            "chunks": [
                {
                    "id": chunk.id,
                    "content": chunk.content,
                    "data_type": chunk.data_type.value,
                    "language": chunk.language.value,
                    "source_metadata": {
                        "file_path": chunk.source_metadata.file_path if chunk.source_metadata else None,
                        "file_name": chunk.source_metadata.file_name if chunk.source_metadata else None,
                        "file_type": chunk.source_metadata.file_type if chunk.source_metadata else None
                    } if chunk.source_metadata else None,
                    "extra_data": chunk.extra_data
                }
                for chunk in data.chunks
            ],
            "qa_pairs": [
                {
                    "question": qa.question,
                    "answer": qa.answer,
                    "context": qa.context,
                    "domain": qa.domain,
                    "confidence": qa.confidence
                }
                for qa in data.qa_pairs
            ],
            "metadata": data.metadata
        }
        
        try:
            if output_file.suffix.lower() == '.json':
                save_json(save_data, output_path)
            else:
                # 默认保存为JSON
                save_json(save_data, str(output_file) + '.json')
            
            self.log_processing_info(f"处理结果保存成功: {output_path}")
            
        except Exception as e:
            self.log_processing_info(f"处理结果保存失败: {e}", "error")
            raise
    
    def create_dataflow_workflow(self, input_path: str, output_path: str) -> Optional[str]:
        """创建DataFlow工作流"""
        if not self.dataflow:
            return None
        
        try:
            # 这里需要根据实际的DataFlow API创建工作流
            # 以下是伪代码示例
            
            workflow_definition = {
                "name": "hydro_data_processing",
                "description": "水利数据处理工作流",
                "steps": [
                    {
                        "name": "parse",
                        "type": "HydroDataParser",
                        "config": config_manager.get("parser", {})
                    },
                    {
                        "name": "clean",
                        "type": "HydroDataCleaner", 
                        "config": config_manager.get("cleaner", {})
                    },
                    {
                        "name": "enhance",
                        "type": "HydroDataEnhancer",
                        "config": config_manager.get("enhancer", {})
                    },
                    {
                        "name": "evaluate",
                        "type": "HydroDataEvaluator",
                        "config": config_manager.get("evaluator", {})
                    }
                ],
                "input": input_path,
                "output": output_path
            }
            
            # 创建工作流
            workflow_id = self.dataflow.create_workflow(workflow_definition)
            
            self.log_processing_info(f"DataFlow工作流创建成功: {workflow_id}")
            return workflow_id
            
        except Exception as e:
            self.log_processing_info(f"DataFlow工作流创建失败: {e}", "error")
            return None
    
    def get_processing_report(self) -> Dict[str, Any]:
        """获取处理报告"""
        report = {
            "pipeline_name": self.name,
            "skills": list(self.skills.keys()),
            "statistics": self.processing_stats.copy(),
            "config": {
                "batch_size": self.pipeline_config.batch_size,
                "max_workers": self.pipeline_config.max_workers,
                "checkpoint_enabled": self.pipeline_config.checkpoint_enabled,
                "error_handling": self.pipeline_config.error_handling,
                "parallel_processing": self.pipeline_config.parallel_processing
            },
            "dataflow_enabled": self.dataflow is not None
        }
        
        # 添加各技能的统计信息
        skill_stats = {}
        for skill_name, skill in self.skills.items():
            if hasattr(skill, 'get_monitoring_info'):
                skill_stats[skill_name] = skill.get_monitoring_info()
        
        report["skill_statistics"] = skill_stats
        
        return report
    
    def reset_statistics(self):
        """重置统计信息"""
        self.processing_stats = {
            "total_files": 0,
            "processed_files": 0,
            "failed_files": 0,
            "total_chunks": 0,
            "processed_chunks": 0,
            "failed_chunks": 0,
            "start_time": None,
            "end_time": None,
            "total_time": 0.0
        }
        
        # 重置技能统计
        for skill in self.skills.values():
            if hasattr(skill, 'reset_statistics'):
                skill.reset_statistics()
        
        self.log_processing_info("统计信息已重置")
    
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """处理单个数据块（接口兼容）"""
        processed_chunk = data_chunk
        
        skill_order = ["cleaner", "enhancer", "evaluator"]
        for skill_name in skill_order:
            if skill_name in self.skills:
                skill = self.skills[skill_name]
                processed_chunk = skill.process_single(processed_chunk)
        
        return processed_chunk
    
    def get_supported_file_formats(self) -> List[str]:
        """获取支持的文件格式"""
        if "parser" in self.skills:
            return self.skills["parser"].get_supported_formats()
        return []
    
    def cleanup_checkpoints(self, older_than_hours: int = 24):
        """清理旧的检查点文件"""
        import datetime
        
        current_time = datetime.datetime.now()
        cleanup_count = 0
        
        for checkpoint_file in self.checkpoint_dir.glob("*.json"):
            file_time = datetime.datetime.fromtimestamp(checkpoint_file.stat().st_mtime)
            
            if (current_time - file_time).total_seconds() > older_than_hours * 3600:
                try:
                    checkpoint_file.unlink()
                    cleanup_count += 1
                except Exception as e:
                    self.log_processing_info(f"清理检查点文件失败: {checkpoint_file} - {e}", "error")
        
        self.log_processing_info(f"清理了 {cleanup_count} 个检查点文件")