#!/usr/bin/env python3
"""
高级使用示例
演示高级功能，包括自定义配置、质量评估、单独使用各组件等
"""

import sys
import os
import json
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.skills.pipeline import HydroDataPipeline
from src.skills.parser import HydroDataParser
from src.skills.cleaner import HydroDataCleaner
from src.skills.enhancer import HydroDataEnhancer
from src.skills.evaluator import HydroDataEvaluator
from src.config import config_manager
from src.utils import setup_logging


def custom_configuration_example():
    """自定义配置示例"""
    print("🔧 自定义配置示例")
    print("-" * 30)
    
    # 创建自定义配置
    custom_config = {
        "global": {
            "log_level": "DEBUG",
            "output_dir": "./custom_output",
            "max_workers": 8
        },
        "parser": {
            "supported_formats": ["txt", "pdf"],
            "text_settings": {
                "chunk_size": 500,
                "overlap": 50
            }
        },
        "cleaner": {
            "remove_duplicates": True,
            "normalize_whitespace": True,
            "min_text_length": 20
        },
        "enhancer": {
            "enable_qa_generation": True,
            "enable_term_extraction": True,
            "domain_knowledge": {
                "hydrology": True,
                "engineering": True
            }
        },
        "evaluator": {
            "quality_metrics": ["completeness", "relevance", "consistency", "diversity"],
            "thresholds": {
                "min_quality_score": 0.8
            }
        },
        "pipeline": {
            "batch_size": 5,
            "parallel_processing": True,
            "checkpoint_enabled": False
        }
    }
    
    # 保存自定义配置
    config_file = "examples/custom_config.yaml"
    import yaml
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(custom_config, f, default_flow_style=False, allow_unicode=True)
    
    print(f"📝 创建自定义配置文件: {config_file}")
    
    # 使用自定义配置
    try:
        # 重新初始化配置管理器
        from src.config import ConfigManager
        custom_config_manager = ConfigManager(config_file)
        
        # 使用自定义配置创建pipeline
        pipeline = HydroDataPipeline()
        
        print("✅ 自定义配置加载成功")
        print(f"   - 批处理大小: {custom_config_manager.get('pipeline.batch_size')}")
        print(f"   - 最大工作线程: {custom_config_manager.get('global.max_workers')}")
        print(f"   - 最低质量评分: {custom_config_manager.get('evaluator.thresholds.min_quality_score')}")
        
    except Exception as e:
        print(f"❌ 自定义配置失败: {e}")
    
    finally:
        # 清理配置文件
        if os.path.exists(config_file):
            os.remove(config_file)


def individual_component_usage():
    """单独使用各组件示例"""
    print("\n🔧 单独使用各组件示例")
    print("-" * 30)
    
    # 创建示例文本
    sample_text = """
    水利工程是国民经济的重要基础设施。大坝是水利工程的核心建筑物，主要用于防洪、发电、灌溉等。
    
    水库调度是水利工程管理的关键环节。通过科学调度，可以实现水资源的优化配置。
    
    什么是洪水？洪水是指江河湖泊水量超过正常水位的现象。防洪措施包括堤防建设、水库调度、分洪工程等。
    """
    
    from src.models import DataChunk, DataType, Language, SourceMetadata
    from datetime import datetime
    
    # 创建示例数据块
    chunk = DataChunk(
        content=sample_text.strip(),
        data_type=DataType.TEXT,
        language=Language.CHINESE
    )
    
    print(f"📝 原始文本长度: {len(chunk.content)} 字符")
    
    # 1. 单独使用Parser
    print("\n1️⃣ 使用 Parser（解析器）")
    try:
        parser = HydroDataParser()
        # Parser主要用于解析文件，这里演示文件解析
        temp_file = "examples/temp_sample.txt"
        Path(temp_file).parent.mkdir(parents=True, exist_ok=True)
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(sample_text)
        
        parsed_chunks = parser.parse_file(temp_file)
        print(f"   解析结果: {len(parsed_chunks)} 个数据块")
        print(f"   检测语言: {parsed_chunks[0].language.value}")
        
        os.remove(temp_file)
        
    except Exception as e:
        print(f"   ❌ Parser使用失败: {e}")
    
    # 2. 单独使用Cleaner
    print("\n2️⃣ 使用 Cleaner（清洗器）")
    try:
        cleaner = HydroDataCleaner()
        cleaned_chunk = cleaner.process_single(chunk)
        
        print(f"   清洗前长度: {len(chunk.content)}")
        print(f"   清洗后长度: {len(cleaned_chunk.content)}")
        print(f"   清洗比率: {len(cleaned_chunk.content)/len(chunk.content):.3f}")
        
        if "cleaned" in cleaned_chunk.extra_data:
            print(f"   清洗标记: {cleaned_chunk.extra_data['cleaned']}")
            
    except Exception as e:
        print(f"   ❌ Cleaner使用失败: {e}")
    
    # 3. 单独使用Enhancer
    print("\n3️⃣ 使用 Enhancer（增强器）")
    try:
        enhancer = HydroDataEnhancer()
        enhanced_chunk = enhancer.process_single(chunk)
        
        if "extracted_terms" in enhanced_chunk.extra_data:
            terms = enhanced_chunk.extra_data["extracted_terms"]
            print(f"   提取术语: {[t['term'] for t in terms[:5]]}")
        
        if "generated_qa" in enhanced_chunk.extra_data:
            qa_list = enhanced_chunk.extra_data["generated_qa"]
            print(f"   生成问答对: {len(qa_list)} 个")
            if qa_list:
                print(f"   示例问题: {qa_list[0]['question']}")
        
        if "domain_tags" in enhanced_chunk.extra_data:
            domains = enhanced_chunk.extra_data["domain_tags"]
            print(f"   领域标签: {domains}")
            
    except Exception as e:
        print(f"   ❌ Enhancer使用失败: {e}")
    
    # 4. 单独使用Evaluator
    print("\n4️⃣ 使用 Evaluator（评估器）")
    try:
        evaluator = HydroDataEvaluator()
        evaluated_chunk = evaluator.process_single(chunk)
        
        if "quality_score" in evaluated_chunk.extra_data:
            quality = evaluated_chunk.extra_data["quality_score"]
            print(f"   总体评分: {quality['overall_score']:.3f}")
            print(f"   完整性评分: {quality['completeness_score']:.3f}")
            print(f"   相关性评分: {quality['relevance_score']:.3f}")
            print(f"   一致性评分: {quality['consistency_score']:.3f}")
            print(f"   多样性评分: {quality['diversity_score']:.3f}")
        
        if "improvement_suggestions" in evaluated_chunk.extra_data:
            suggestions = evaluated_chunk.extra_data["improvement_suggestions"]
            print(f"   改进建议: {suggestions}")
            
    except Exception as e:
        print(f"   ❌ Evaluator使用失败: {e}")


def quality_assessment_demo():
    """质量评估演示"""
    print("\n📊 质量评估详细演示")
    print("-" * 30)
    
    # 创建不同质量的示例文本
    quality_examples = {
        "high_quality": """
        水利工程中的水库调度是一个复杂的多目标优化问题。现代水库调度需要综合考虑防洪、发电、灌溉、供水等多重需求。
        
        从技术角度看，水库调度涉及水文预报、入库流量计算、出库控制等多个环节。其中，水文预报精度直接影响调度效果。
        
        目前常用的调度方法包括规则调度、优化调度和智能调度三大类。优化调度又可分为线性规划、非线性规划、动态规划等方法。
        
        实践表明，结合人工智能技术的智能调度方法能够显著提高调度效率和经济效益。例如，基于神经网络的调度模型在多个水库中取得了良好效果。
        """,
        
        "medium_quality": "水库调度很重要。要考虑防洪和发电。用水需要合理安排。调度方法有很多种。",
        
        "low_quality": "水。库。调。度。"
    }
    
    evaluator = HydroDataEvaluator()
    
    for quality_name, text in quality_examples.items():
        print(f"\n📝 {quality_name.replace('_', ' ').title()}:")
        print(f"   文本长度: {len(text)} 字符")
        
        from src.models import DataChunk, DataType, Language
        chunk = DataChunk(
            content=text.strip(),
            data_type=DataType.TEXT,
            language=Language.CHINESE
        )
        
        evaluated_chunk = evaluator.process_single(chunk)
        
        if "quality_score" in evaluated_chunk.extra_data:
            quality = evaluated_chunk.extra_data["quality_score"]
            print(f"   总体评分: {quality['overall_score']:.3f}")
            print(f"   完整性: {quality['completeness_score']:.3f}")
            print(f"   相关性: {quality['relevance_score']:.3f}")
            print(f"   一致性: {quality['consistency_score']:.3f}")
            print(f"   多样性: {quality['diversity_score']:.3f}")


def knowledge_base_demo():
    """知识库演示"""
    print("\n🧠 知识库演示")
    print("-" * 30)
    
    try:
        enhancer = HydroDataEnhancer()
        knowledge_base = enhancer.get_knowledge_base()
        
        print(f"📚 知识库统计:")
        print(f"   术语数量: {len(knowledge_base.terms)}")
        print(f"   实体数量: {len(knowledge_base.entities)}")
        print(f"   关系数量: {len(knowledge_base.relationships)}")
        
        print(f"\n🏷️  部分术语示例:")
        for i, (term, aliases) in enumerate(list(knowledge_base.terms.items())[:5]):
            print(f"   {i+1}. {term}: {aliases}")
        
        print(f"\n🏢 部分实体示例:")
        for i, (entity_id, entity_info) in enumerate(list(knowledge_base.entities.items())[:3]):
            print(f"   {i+1}. {entity_id}: {entity_info}")
        
        print(f"\n🔗 部分关系示例:")
        for i, relation in enumerate(knowledge_base.relationships[:3]):
            print(f"   {i+1}. {relation['subject']} -> {relation['relation']} -> {relation['object']}")
        
        # 演示更新知识库
        print(f"\n➕ 更新知识库演示:")
        new_terms = ["水利工程", "水资源管理", "水生态保护"]
        new_entities = {
            "南水北调": {"type": "调水工程", "length": "4350km"},
            "三峡大坝": {"type": "混凝土重力坝", "height": "185m"}
        }
        
        enhancer.update_knowledge_base(new_terms, new_entities)
        
        updated_kb = enhancer.get_knowledge_base()
        print(f"   更新后术语数量: {len(updated_kb.terms)}")
        print(f"   更新后实体数量: {len(updated_kb.entities)}")
        
    except Exception as e:
        print(f"❌ 知识库演示失败: {e}")


def performance_monitoring_demo():
    """性能监控演示"""
    print("\n📈 性能监控演示")
    print("-" * 30)
    
    # 创建多个示例文件
    sample_dir = Path("examples/performance_samples")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    sample_files = []
    for i in range(5):
        file_path = sample_dir / f"sample_{i+1}.txt"
        content = f"""
        这是第{i+1}个示例文件。水利工程第{i+1}部分内容。
        
        关键技术指标{i+1}：
        - 指标A: {i+1}0.5
        - 指标B: {i+1}2.3
        - 指标C: {i+1}8.7
        
        这个文件用于演示性能监控功能。包含{i+1}个不同的数据点。
        """
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
        sample_files.append(str(file_path))
    
    try:
        pipeline = HydroDataPipeline()
        
        # 处理文件
        result = pipeline.process_files(
            file_paths=sample_files,
            output_path="examples/performance_result.json"
        )
        
        if result.success:
            # 获取处理报告
            report = pipeline.get_processing_report()
            
            print("📊 性能监控报告:")
            print(f"   总文件数: {report['statistics']['total_files']}")
            print(f"   成功处理: {report['statistics']['processed_files']}")
            print(f"   失败文件: {report['statistics']['failed_files']}")
            print(f"   总数据块: {report['statistics']['total_chunks']}")
            print(f"   处理时间: {report['statistics']['total_time']:.2f}秒")
            
            # 各技能统计
            print(f"\n🔧 各技能统计:")
            for skill_name, stats in report['skill_statistics'].items():
                print(f"   {skill_name}:")
                print(f"     处理次数: {stats.get('processed_count', 0)}")
                print(f"     失败次数: {stats.get('failed_count', 0)}")
                print(f"     成功率: {stats.get('success_rate', 0):.1f}%")
                print(f"     平均处理时间: {stats.get('average_processing_time', 0):.3f}秒")
        
    except Exception as e:
        print(f"❌ 性能监控演示失败: {e}")
    
    finally:
        # 清理
        import shutil
        if sample_dir.exists():
            shutil.rmtree(sample_dir)


def main():
    """主函数"""
    setup_logging("INFO")
    
    print("=" * 60)
    print("水利数据处理工具 - 高级使用示例")
    print("=" * 60)
    
    # 1. 自定义配置示例
    custom_configuration_example()
    
    # 2. 单独使用各组件示例
    individual_component_usage()
    
    # 3. 质量评估演示
    quality_assessment_demo()
    
    # 4. 知识库演示
    knowledge_base_demo()
    
    # 5. 性能监控演示
    performance_monitoring_demo()
    
    print("\n" + "=" * 60)
    print("高级使用示例运行完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()