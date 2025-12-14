#!/usr/bin/env python3
"""
批量处理示例
演示如何批量处理多个文件和目录
"""

import sys
import os
import time
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.skills.pipeline import HydroDataPipeline
from src.config import config_manager
from src.utils import setup_logging


def create_batch_sample_files():
    """创建批量处理的示例文件"""
    samples_dir = Path("examples/batch_samples")
    samples_dir.mkdir(parents=True, exist_ok=True)
    
    # 示例文件内容
    sample_files = {
        "hydrology_basics.txt": """
水文基础知识

水文是研究水的各种现象和规律的科学。主要包括降水、蒸发、径流、地下水等水文要素。

降水量是指在一定时间段内降落到地面的水量，通常用毫米(mm)表示。中国年降水量分布不均，东南沿海地区年降水量可达2000mm以上，而西北地区不足200mm。

径流是指降水在地面汇集后形成的水流。径流量是衡量水资源丰富程度的重要指标。
        """,
        
        "dam_engineering.txt": """
大坝工程技术

大坝是水利工程的重要组成部分，主要用于防洪、发电、灌溉等目的。

按照建筑材料分类，大坝可分为土石坝、混凝土坝、砌石坝等。其中混凝土坝又分为重力坝、拱坝、支墩坝等类型。

大坝安全监测包括变形监测、渗流监测、应力监测等多个方面。
        """,
        
        "flood_control.txt": """
防洪工程体系

防洪是水利工程的重要任务之一。防洪工程体系包括堤防、水库、蓄滞洪区、分洪道等。

堤防是防止洪水泛滥的主要工程措施，按照保护对象可分为城市堤防、农村堤防等。

水库调度是防洪的重要手段，通过合理调度水库库容，可以有效削减洪峰。
        """,
        
        "water_quality.txt": """
水质监测与保护

水质是指水的物理、化学和生物特性。水质监测是水环境保护的重要基础。

主要水质指标包括pH值、溶解氧、氨氮、总磷、化学需氧量等。

地表水环境质量标准将水质分为五类，其中I类水质最好，V类水质最差。
        """
    }
    
    created_files = []
    for filename, content in sample_files.items():
        file_path = samples_dir / filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
        created_files.append(str(file_path))
        print(f"📝 创建示例文件: {file_path}")
    
    return created_files


def batch_process_files():
    """批量处理文件的示例"""
    # 设置日志
    setup_logging("INFO")
    
    # 创建示例文件
    print("📁 创建批量处理示例文件...")
    sample_files = create_batch_sample_files()
    
    try:
        # 初始化pipeline
        print("\n🚀 初始化水利数据处理工具...")
        
        # 配置批量处理参数
        config_manager.set("pipeline.batch_size", 2)
        config_manager.set("pipeline.max_workers", 2)
        
        pipeline = HydroDataPipeline()
        
        # 批量处理文件
        print(f"📄 批量处理 {len(sample_files)} 个文件...")
        start_time = time.time()
        
        result = pipeline.process_files(
            file_paths=sample_files,
            output_path="examples/batch_processing_result.json"
        )
        
        processing_time = time.time() - start_time
        
        # 显示结果
        if result.success:
            print("\n✅ 批量处理成功!")
            print(f"⏱️  总处理时间: {processing_time:.2f}秒")
            print(f"📊 平均每个文件处理时间: {processing_time/len(sample_files):.2f}秒")
            
            if result.data:
                stats = result.data.get_statistics()
                print(f"\n📈 批量处理统计:")
                print(f"   - 总数据块数量: {stats['total_chunks']}")
                print(f"   - 总问答对数量: {stats['total_qa_pairs']}")
                print(f"   - 总字符数: {stats['total_characters']}")
                print(f"   - 数据类型分布: {stats['data_types']}")
                print(f"   - 语言分布: {stats['languages']}")
                
                # 按文件类型分析
                print(f"\n📋 详细分析:")
                for chunk in result.data.chunks:
                    source_name = Path(chunk.source_metadata.file_name).stem if chunk.source_metadata else "unknown"
                    print(f"   - {source_name}: {len(chunk.content)} 字符")
                    
                    if "quality_score" in chunk.extra_data:
                        quality = chunk.extra_data["quality_score"]
                        print(f"     质量评分: {quality['overall_score']:.3f}")
                
                # 质量评估报告
                evaluator_stats = pipeline.skills.get("evaluator")
                if evaluator_stats:
                    report = evaluator_stats.get_evaluation_report()
                    print(f"\n📊 质量评估报告:")
                    if "overall_stats" in report:
                        overall = report["overall_stats"]
                        print(f"   - 平均质量评分: {overall['mean']:.3f}")
                        print(f"   - 质量评分标准差: {overall['std']:.3f}")
                        print(f"   - 最低评分: {overall['min']:.3f}")
                        print(f"   - 最高评分: {overall['max']:.3f}")
        else:
            print(f"❌ 批量处理失败: {result.error_message}")
            
    except Exception as e:
        print(f"💥 批量处理过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理示例文件
        samples_dir = Path("examples/batch_samples")
        if samples_dir.exists():
            import shutil
            shutil.rmtree(samples_dir)
            print(f"\n🗑️  清理示例目录: {samples_dir}")


def process_directory_example():
    """处理目录的示例"""
    print("\n" + "="*50)
    print("目录处理示例")
    print("="*50)
    
    # 使用已创建的示例目录
    samples_dir = Path("examples/batch_samples")
    if not samples_dir.exists():
        print("❌ 示例目录不存在，请先运行批量处理示例")
        return
    
    try:
        # 初始化pipeline
        pipeline = HydroDataPipeline()
        
        # 处理整个目录
        print(f"📁 处理目录: {samples_dir}")
        result = pipeline.process_directory(
            directory_path=str(samples_dir),
            recursive=False,  # 不递归处理子目录
            output_path="examples/directory_processing_result.json"
        )
        
        if result.success:
            print("✅ 目录处理成功!")
            if result.data:
                stats = result.data.get_statistics()
                print(f"📊 目录处理统计:")
                print(f"   - 数据块数量: {stats['total_chunks']}")
                print(f"   - 问答对数量: {stats['total_qa_pairs']}")
        else:
            print(f"❌ 目录处理失败: {result.error_message}")
            
    except Exception as e:
        print(f"💥 目录处理过程中出现异常: {e}")


def parallel_processing_comparison():
    """并行处理性能对比示例"""
    print("\n" + "="*50)
    print("并行处理性能对比")
    print("="*50)
    
    # 重新创建示例文件
    sample_files = create_batch_sample_files()
    
    try:
        # 串行处理
        print("🔄 串行处理测试...")
        config_manager.set("pipeline.max_workers", 1)
        config_manager.set("pipeline.parallel_processing", False)
        
        pipeline_serial = HydroDataPipeline()
        start_time = time.time()
        
        result_serial = pipeline_serial.process_files(sample_files)
        serial_time = time.time() - start_time
        
        if result_serial.success:
            print(f"   串行处理时间: {serial_time:.2f}秒")
        
        # 并行处理
        print("\n🚀 并行处理测试...")
        config_manager.set("pipeline.max_workers", 4)
        config_manager.set("pipeline.parallel_processing", True)
        
        pipeline_parallel = HydroDataPipeline()
        start_time = time.time()
        
        result_parallel = pipeline_parallel.process_files(sample_files)
        parallel_time = time.time() - start_time
        
        if result_parallel.success:
            print(f"   并行处理时间: {parallel_time:.2f}秒")
            
            # 性能对比
            if serial_time > 0:
                speedup = serial_time / parallel_time
                print(f"\n📈 性能对比:")
                print(f"   - 串行时间: {serial_time:.2f}秒")
                print(f"   - 并行时间: {parallel_time:.2f}秒")
                print(f"   - 加速比: {speedup:.2f}x")
                print(f"   - 效率提升: {(speedup-1)/speedup*100:.1f}%")
        
    except Exception as e:
        print(f"💥 性能对比测试失败: {e}")
    
    finally:
        # 清理
        samples_dir = Path("examples/batch_samples")
        if samples_dir.exists():
            import shutil
            shutil.rmtree(samples_dir)


if __name__ == "__main__":
    print("=" * 60)
    print("水利数据处理工具 - 批量处理示例")
    print("=" * 60)
    
    # 1. 批量文件处理
    batch_process_files()
    
    # 2. 目录处理示例
    process_directory_example()
    
    # 3. 并行处理性能对比
    parallel_processing_comparison()
    
    print("\n" + "=" * 60)
    print("批量处理示例运行完成!")
    print("=" * 60)