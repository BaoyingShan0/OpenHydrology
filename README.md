# 水利多模态数据处理工具

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

一个专门为水利大模型准备训练数据的Claude Skill工具，能够自动处理多种格式的水利数据，包括PDF文档、纯文本、低质量问答等，从中解析、清洗、增强并评估高质量数据。

## 🌟 主要特性

### 🔄 完整的数据处理流程
- **HydroData-Parser**: 支持多种文件格式解析（PDF、TXT、JSON、CSV、MD）
- **HydroData-Cleaner**: 智能数据清洗和去噪
- **HydroData-Enhancer**: 数据增强和问答对生成
- **HydroData-Evaluator**: 多维度质量评估
- **HydroData-Pipeline**: 完整流程控制和协调

### 🚀 强大的功能特性
- ✅ **多语言支持**: 中文、英文自动识别和处理
- ✅ **智能去重**: 基于哈希和相似度的重复数据检测
- ✅ **专业术语**: 水利领域知识库和专业术语提取
- ✅ **问答生成**: 自动生成高质量问答对
- ✅ **质量评估**: 完整性、相关性、一致性、多样性多维度评估
- ✅ **并行处理**: 支持多线程并行处理提升效率
- ✅ **检查点**: 支持断点续传，处理大数据更安全
- ✅ **DataFlow集成**: 支持DataFlow工作流引擎

### 📊 支持的数据格式
- 📄 **PDF文档**: 自动提取文本、表格内容
- 📝 **纯文本**: 支持多种编码自动检测
- 📊 **JSON结构**: 递归解析嵌套数据结构
- 📈 **CSV表格**: 智能解析表格数据
- 📑 **Markdown**: 支持Markdown格式文档

## 🛠️ 安装和使用

### 环境要求
- Python 3.8+
- 推荐使用虚拟环境

### 安装依赖
```bash
# 克隆项目
git clone <repository-url>
cd OpenHydrology_data

# 安装依赖
pip install -r requirements.txt
```

### 可选依赖
```bash
# PDF处理支持
pip install PyPDF2 pdfplumber

# 高级NLP功能
pip install spacy transformers
python -m spacy download en_core_web_sm

# 中文分词支持
pip install jieba

# 语言检测
pip install langdetect

# 数据科学工具
pip install scikit-learn pandas numpy

# 高级文本处理
pip install chardet
```

## 🚀 快速开始

### 1. 基本使用
```bash
# 处理单个文件
python main.py --input data/report.pdf --output result.json

# 处理整个目录
python main.py --input ./documents --output results.json --recursive

# 使用自定义配置
python main.py --config custom_config.yaml --input ./data --output result.json
```

### 2. 高级选项
```bash
# 设置批处理大小和工作线程
python main.py --input ./data --batch-size 50 --workers 8

# 只生成报告不处理
python main.py --input ./data --report-only

# 查看支持的文件格式
python main.py --list-formats
```

### 3. 编程接口使用
```python
from src.skills.pipeline import HydroDataPipeline
from src.config import config_manager

# 初始化pipeline
pipeline = HydroDataPipeline()

# 处理文件
result = pipeline.process_files("data/report.pdf", "output.json")

# 处理目录
result = pipeline.process_directory("./documents", recursive=True)

# 获取处理报告
report = pipeline.get_processing_report()
```

## 📋 配置文件说明

主要配置文件 `config/hydro_config.yaml`:

```yaml
# 全局设置
global:
  log_level: INFO
  output_dir: ./output
  temp_dir: ./temp
  max_workers: 4

# 解析器配置
parser:
  supported_formats: [pdf, txt, json, csv, md]
  text_settings:
    chunk_size: 1000
    overlap: 100
  pdf_settings:
    extract_tables: true
    min_confidence: 0.8

# 清洗器配置
cleaner:
  remove_duplicates: true
  normalize_whitespace: true
  min_text_length: 10

# 增强器配置
enhancer:
  enable_qa_generation: true
  enable_term_extraction: true
  
# 评估器配置
evaluator:
  quality_metrics: [completeness, relevance, consistency, diversity]
  thresholds:
    min_quality_score: 0.7

# 流程控制配置
pipeline:
  batch_size: 100
  parallel_processing: true
  checkpoint_enabled: true
```

## 📊 处理结果格式

处理后的数据保存在JSON格式中，包含以下结构：

```json
{
  "id": "processed_data_id",
  "name": "processed_data_20231214_120000",
  "description": "处理数据的描述",
  "statistics": {
    "total_chunks": 150,
    "total_qa_pairs": 75,
    "total_characters": 50000,
    "data_types": {"pdf": 100, "text": 50},
    "languages": {"zh": 120, "en": 30}
  },
  "chunks": [
    {
      "id": "chunk_id",
      "content": "处理后的文本内容",
      "data_type": "text",
      "language": "zh",
      "extra_data": {
        "extracted_terms": [...],
        "quality_score": {...},
        "generated_qa": [...]
      }
    }
  ],
  "qa_pairs": [
    {
      "question": "什么是水文？",
      "answer": "水文是研究水的各种现象和规律的科学...",
      "context": "相关上下文",
      "domain": "水文学",
      "confidence": 0.85
    }
  ]
}
```

## 🔧 核心组件详解

### HydroData-Parser 数据解析器

负责解析不同格式的原始数据文件，支持多种编码检测和语言识别。

**主要功能:**
- 多格式文件解析（PDF、TXT、JSON、CSV、MD）
- 自动编码检测
- 多语言支持
- 大文件分块处理
- 元数据提取

**使用示例:**
```python
from src.skills.parser import HydroDataParser

parser = HydroDataParser()
chunks = parser.parse_file("data/report.pdf")
# 返回 DataChunk 对象列表
```

### HydroData-Cleaner 数据清洗器

去除数据噪声，标准化格式，提升数据质量。

**主要功能:**
- 重复数据检测和去除
- 文本格式标准化
- 特殊字符处理
- 质量过滤
- 多语言清洗

**使用示例:**
```python
from src.skills.cleaner import HydroDataCleaner

cleaner = HydroDataCleaner()
cleaned_chunks = cleaner.process_batch(chunks)
```

### HydroData-Enhancer 数据增强器

丰富数据内容，生成问答对，提取专业术语。

**主要功能:**
- 专业术语提取
- 问答对自动生成
- 知识图谱构建
- 领域知识关联
- 多模态增强

**使用示例:**
```python
from src.skills.enhancer import HydroDataEnhancer

enhancer = HydroDataEnhancer()
enhanced_chunks = enhancer.process_batch(cleaned_chunks)
```

### HydroData-Evaluator 数据评估器

多维度评估数据质量，提供改进建议。

**评估维度:**
- **完整性**: 内容长度、结构、信息密度
- **相关性**: 水利领域相关性、专业术语比例
- **一致性**: 语言一致性、术语使用一致性
- **多样性**: 词汇多样性、句式多样性、主题多样性

**使用示例:**
```python
from src.skills.evaluator import HydroDataEvaluator

evaluator = HydroDataEvaluator()
evaluated_chunks = evaluator.process_batch(enhanced_chunks)
report = evaluator.get_evaluation_report()
```

### HydroData-Pipeline 流程控制器

协调所有组件，提供完整的处理流程。

**主要功能:**
- 流程编排和协调
- 批量和并行处理
- 检查点管理
- 错误处理和恢复
- DataFlow工作流集成

## 📈 性能优化建议

### 1. 硬件优化
- **CPU**: 多核处理器，推荐8核以上
- **内存**: 16GB以上，处理大文件时建议32GB
- **存储**: SSD硬盘，提升I/O性能

### 2. 软件配置
- **并行处理**: 根据CPU核心数设置合适的worker数量
- **批处理大小**: 根据内存大小调整batch_size
- **检查点**: 处理大数据时启用检查点功能

### 3. 配置调优
```yaml
pipeline:
  batch_size: 200        # 增大批处理大小
  max_workers: 8         # 增加并行线程
  parallel_processing: true
  
global:
  max_workers: 8         # 全局并行设置
```

## 🐛 常见问题和解决方案

### Q1: PDF解析失败
**原因**: 缺少PDF处理库或PDF文件损坏
**解决**: 
```bash
pip install PyPDF2 pdfplumber
# 检查PDF文件是否可正常打开
```

### Q2: 中文显示乱码
**原因**: 编码检测失败
**解决**: 
```yaml
parser:
  text_settings:
    encoding_detection: true
```

### Q3: 内存不足
**原因**: 处理大文件时内存占用过高
**解决**:
```yaml
pipeline:
  batch_size: 50        # 减小批处理大小
  max_workers: 2        # 减少并行线程
```

### Q4: 处理速度慢
**原因**: 单线程处理或数据量大
**解决**:
```yaml
pipeline:
  parallel_processing: true
  max_workers: 8
```

## 📚 API参考

### HydroDataPipeline

主要的数据处理流程控制类。

**方法:**
- `process_files(file_paths, output_path)` - 处理文件列表
- `process_directory(directory_path, recursive, output_path)` - 处理目录
- `get_processing_report()` - 获取处理报告
- `get_supported_file_formats()` - 获取支持的格式

### DataChunk

数据块基类，包含处理的基本单元。

**属性:**
- `id`: 唯一标识符
- `content`: 文本内容
- `data_type`: 数据类型（PDF、TEXT等）
- `language`: 语言类型
- `extra_data`: 额外数据（术语、质量评分等）

### QualityScore

质量评分对象。

**属性:**
- `overall_score`: 总体评分
- `completeness_score`: 完整性评分
- `relevance_score`: 相关性评分
- `consistency_score`: 一致性评分
- `diversity_score`: 多样性评分

## 🤝 贡献指南

欢迎贡献代码和改进建议！

### 开发环境设置
```bash
# 克隆项目
git clone <repository-url>
cd OpenHydrology_data

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\\Scripts\\activate   # Windows

# 安装依赖
pip install -r requirements.txt
```

### 运行测试
```bash
# 运行单元测试
python -m pytest tests/

# 运行代码质量检查
flake8 src/
black src/
```

### 提交规范
- feat: 新功能
- fix: 修复bug
- docs: 文档更新
- style: 代码格式调整
- refactor: 代码重构
- test: 测试相关
- chore: 构建过程或辅助工具的变动

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [DataFlow](https://github.com/OpenDCAI/DataFlow) - 工作流引擎支持
- [spaCy](https://spacy.io/) - 自然语言处理
- [Transformers](https://huggingface.co/transformers/) - 预训练模型
- [jieba](https://github.com/fxsjy/jieba) - 中文分词

## 📞 联系方式

如有问题或建议，请通过以下方式联系：

- 📧 Email: [your-email@example.com]
- 🐛 Issues: [GitHub Issues](https://github.com/your-repo/issues)
- 📖 文档: [项目文档](https://your-docs-site.com)

---

**让水利数据处理更简单，让AI理解水利世界！** 🌊