"""
数据模型模块
定义数据处理中使用的各种数据结构
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from enum import Enum
import uuid


class DataType(Enum):
    """数据类型枚举"""
    PDF = "pdf"
    TEXT = "text"
    QA = "qa"
    TABLE = "table"
    IMAGE = "image"
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"


class ProcessingStatus(Enum):
    """处理状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class Language(Enum):
    """语言枚举"""
    CHINESE = "zh"
    ENGLISH = "en"
    AUTO = "auto"


@dataclass
class SourceMetadata:
    """源数据元信息"""
    file_path: str
    file_name: str
    file_size: int
    file_type: str
    encoding: Optional[str] = None
    hash_value: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None


@dataclass
class ProcessMetadata:
    """处理过程元信息"""
    processor_name: str
    processing_time: float
    start_time: datetime
    end_time: datetime
    status: ProcessingStatus
    error_message: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataChunk:
    """数据块"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content: str = ""
    data_type: DataType = DataType.TEXT
    language: Language = Language.AUTO
    source_metadata: Optional[SourceMetadata] = None
    process_metadata: List[ProcessMetadata] = field(default_factory=list)
    extra_data: Dict[str, Any] = field(default_factory=dict)
    
    def add_process_metadata(self, metadata: ProcessMetadata):
        """添加处理元信息"""
        self.process_metadata.append(metadata)
    
    def get_latest_status(self) -> ProcessingStatus:
        """获取最新处理状态"""
        if not self.process_metadata:
            return ProcessingStatus.PENDING
        return self.process_metadata[-1].status


@dataclass
class QAData:
    """问答数据结构"""
    question: str
    answer: str
    context: Optional[str] = None
    difficulty: Optional[str] = None  # easy, medium, hard
    domain: Optional[str] = None  # 水利专业领域
    confidence: Optional[float] = None  # 置信度
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityScore:
    """质量评分"""
    overall_score: float
    completeness_score: float
    relevance_score: float
    consistency_score: float
    diversity_score: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessedData:
    """处理后的数据集"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    chunks: List[DataChunk] = field(default_factory=list)
    qa_pairs: List[QAData] = field(default_factory=list)
    quality_score: Optional[QualityScore] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_chunk(self, chunk: DataChunk):
        """添加数据块"""
        self.chunks.append(chunk)
        self.updated_at = datetime.now()
    
    def add_qa_pair(self, qa: QAData):
        """添加问答对"""
        self.qa_pairs.append(qa)
        self.updated_at = datetime.now()
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取数据统计信息"""
        return {
            "total_chunks": len(self.chunks),
            "total_qa_pairs": len(self.qa_pairs),
            "data_types": {
                dtype.value: sum(1 for chunk in self.chunks if chunk.data_type == dtype)
                for dtype in DataType
            },
            "languages": {
                lang.value: sum(1 for chunk in self.chunks if chunk.language == lang)
                for lang in Language
            },
            "total_characters": sum(len(chunk.content) for chunk in self.chunks),
            "average_chunk_length": sum(len(chunk.content) for chunk in self.chunks) / len(self.chunks) if self.chunks else 0
        }


@dataclass
class ProcessingResult:
    """处理结果"""
    success: bool
    data: Optional[ProcessedData] = None
    error_message: Optional[str] = None
    processing_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineConfig:
    """流水线配置"""
    
    def __init__(self):
        self.parser_enabled: bool = True
        self.cleaner_enabled: bool = True
        self.enhancer_enabled: bool = True
        self.evaluator_enabled: bool = True
        
        self.parser_params: Dict[str, Any] = {}
        self.cleaner_params: Dict[str, Any] = {}
        self.enhancer_params: Dict[str, Any] = {}
        self.evaluator_params: Dict[str, Any] = {}
        
        self.batch_size: int = 100
        self.max_workers: int = 4
        self.checkpoint_enabled: bool = True
        self.error_handling: str = "skip"  # skip, stop, retry


class KnowledgeBase:
    """知识库类"""
    
    def __init__(self):
        self.terms: Dict[str, List[str]] = {}  # 术语映射
        self.entities: Dict[str, Dict[str, Any]] = {}  # 实体信息
        self.relationships: List[Dict[str, Any]] = []  # 关系列表
    
    def add_term(self, term: str, aliases: List[str] = None):
        """添加术语"""
        self.terms[term] = aliases or []
    
    def add_entity(self, entity_id: str, entity_info: Dict[str, Any]):
        """添加实体"""
        self.entities[entity_id] = entity_info
    
    def add_relationship(self, subject: str, relation: str, object_: str):
        """添加关系"""
        self.relationships.append({
            "subject": subject,
            "relation": relation,
            "object": object_
        })