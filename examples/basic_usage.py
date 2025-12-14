#!/usr/bin/env python3
"""
基本使用示例
演示如何使用水利数据处理工具处理单个文件
"""

import sys
import os
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.skills.pipeline import HydroDataPipeline
from src.config import config_manager
from src.utils import setup_logging


def process_single_file():
    """处理单个文件的示例"""
    # 设置日志
    setup_logging("INFO")
    
    # 创建示例数据文件
    sample_file = "examples/sample_hydro_data.txt"
    create_sample_file(sample_file)
    
    try:
        # 初始化pipeline
        print("🚀 初始化水利数据处理工具...")
        pipeline = HydroDataPipeline()
        
        # 处理文件
        print(f"📄 处理文件: {sample_file}")
        result = pipeline.process_files(
            file_paths=[sample_file],
            output_path="examples/processed_single_file.json"
        )
        
        # 显示结果
        if result.success:
            print("✅ 处理成功!")
            print(f"⏱️  处理时间: {result.processing_time:.2f}秒")
            
            if result.data:
                stats = result.data.get_statistics()
                print(f"📊 处理统计:")
                print(f"   - 数据块数量: {stats['total_chunks']}")
                print(f"   - 问答对数量: {stats['total_qa_pairs']}")
                print(f"   - 总字符数: {stats['total_characters']}")
                
                # 显示处理后的数据样例
                if result.data.chunks:
                    print("\n📝 处理后的数据样例:")
                    chunk = result.data.chunks[0]
                    print(f"内容长度: {len(chunk.content)} 字符")
                    print(f"语言: {chunk.language.value}")
                    print(f"数据类型: {chunk.data_type.value}")
                    
                    if "extracted_terms" in chunk.extra_data:
                        terms = chunk.extra_data["extracted_terms"]
                        print(f"提取的专业术语: {[t['term'] for t in terms[:3]]}")
                    
                    if "quality_score" in chunk.extra_data:
                        quality = chunk.extra_data["quality_score"]
                        print(f"质量评分: {quality['overall_score']:.3f}")
                
                # 显示问答对样例
                if result.data.qa_pairs:
                    print("\n❓ 生成的问答对样例:")
                    qa = result.data.qa_pairs[0]
                    print(f"Q: {qa.question}")
                    print(f"A: {qa.answer}")
                    print(f"置信度: {qa.confidence:.2f}")
        else:
            print(f"❌ 处理失败: {result.error_message}")
            
    except Exception as e:
        print(f"💥 处理过程中出现异常: {e}")
    
    finally:
        # 清理示例文件
        if os.path.exists(sample_file):
            os.remove(sample_file)
            print(f"🗑️  清理示例文件: {sample_file}")


def create_sample_file(file_path: str):
    """创建示例水利数据文件"""
    sample_content = """
水利工程质量控制要点

一、概述
水利工程是国民经济的重要基础设施，其质量控制直接关系到工程安全和效益。本文主要介绍水利工程质量控制的关键要点和技术措施。

二、质量控制体系
建立完善的质量控制体系是保证工程质量的基础。质量控制体系包括：
1. 质量管理制度
2. 技术标准体系  
3. 检测监测体系
4. 评价反馈体系

三、关键技术指标
3.1 混凝土强度
混凝土强度是水利工程的重要指标，一般要求C30以上。抗压强度试验应在28天后进行，强度值不低于设计强度的95%。

3.2 渗透系数
土石坝的渗透系数应控制在1×10^-7 cm/s以下，确保坝体的防渗性能。

3.3 变形监测
大坝变形监测包括垂直位移和水平位移两个方向。年变形量应控制在设计允许范围内，一般不超过坝高的0.1%。

四、施工质量控制
4.1 材料控制
- 水泥：采用P.O 42.5以上普通硅酸盐水泥
- 骨料：粒径、级配符合设计要求
- 钢筋：力学性能符合GB1499标准

4.2 工艺控制
混凝土浇筑应分层进行，每层厚度不超过30cm。振捣应充分，避免出现蜂窝、麻面等缺陷。

五、质量检测方法
5.1 无损检测
采用超声波检测混凝土内部质量，回弹法检测表面强度。

5.2 取样检测
在施工现场随机取样，制作试块进行室内试验检测。

六、常见问题及处理
6.1 裂缝问题
混凝土裂缝是常见质量问题，应分析裂缝成因，采取相应的修补措施。

6.2 渗漏问题
渗漏问题严重影响工程安全，需要及时进行防渗处理。

七、总结
水利工程质量控制是一个系统工程，需要从设计、施工、监测等各个环节严格控制，确保工程安全可靠运行。
    """
    
    # 确保目录存在
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    
    # 写入示例内容
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(sample_content.strip())
    
    print(f"📝 创建示例文件: {file_path}")


if __name__ == "__main__":
    print("=" * 50)
    print("水利数据处理工具 - 基本使用示例")
    print("=" * 50)
    
    process_single_file()
    
    print("\n" + "=" * 50)
    print("示例运行完成!")
    print("=" * 50)