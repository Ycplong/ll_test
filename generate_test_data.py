import os
import random
import hashlib
from datetime import datetime

# 生成随机图像数据的函数
def generate_random_image(width=600, height=400):
    from PIL import Image, ImageDraw
    
    # 创建一个随机背景色的图像
    img = Image.new('RGB', (width, height), 
                   (random.randint(200, 255), random.randint(200, 255), random.randint(200, 255)))
    draw = ImageDraw.Draw(img)
    
    # 添加随机噪点
    for _ in range(width * height // 10):
        x = random.randint(0, width - 1)
        y = random.randint(0, height - 1)
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        draw.point((x, y), fill=color)
    
    # 添加模拟缺陷点（1-5个）
    for _ in range(random.randint(1, 5)):
        x = random.randint(50, width - 50)
        y = random.randint(50, height - 50)
        size = random.randint(5, 20)
        color = (random.randint(0, 100), random.randint(0, 100), random.randint(0, 100))
        draw.ellipse((x - size, y - size, x + size, y + size), fill=color)
    
    return img

# 生成晶圆数据
def generate_wafer_data(base_path, wafer_prefix, count, create_nested=False):
    # 使用传入的前缀作为晶圆类型，而不是硬编码列表
    wafer_types = [wafer_prefix]
    
    for wafer_type in wafer_types:
        for i in range(1, count + 1):
            wafer_name = f"{wafer_type}-{i:02d}"
            wafer_dir = os.path.join(base_path, wafer_name)
            
            # 创建晶圆文件夹
            os.makedirs(wafer_dir, exist_ok=True)
            
            # 生成明场和暗场图像
            bright_field_img = generate_random_image()
            bright_field_img.save(os.path.join(wafer_dir, 'bright_field.png'))
            
            dark_field_img = generate_random_image()
            dark_field_img.save(os.path.join(wafer_dir, 'dark_field.png'))
            
            # 生成raw_data.txt文件
            defect_count = random.randint(10, 20)
            with open(os.path.join(wafer_dir, 'raw_data.txt'), 'w') as f:
                f.write("defect_id,center_x,center_y,ai_adc_type\n")
                for j in range(1, defect_count + 1):
                    defect_id = f"DEF_{wafer_type}{i:02d}_{j:03d}"
                    center_x = random.randint(100, 500)
                    center_y = random.randint(100, 300)
                    ai_adc_type = random.randint(1, 5)  # 1-5的缺陷类型
                    f.write(f"{defect_id},{center_x},{center_y},{ai_adc_type}\n")
            
            print(f"已生成晶圆数据: {wafer_name}")

if __name__ == "__main__":
    # 当前目录作为基础路径
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    # 生成A-01~A-05、B-01~B-05和C-01~C-05，共15个晶圆
    generate_wafer_data(base_path, 'A', 5)
    generate_wafer_data(base_path, 'B', 5)
    generate_wafer_data(base_path, 'C', 5)
    
    # 在A-01文件夹下创建嵌套的D-01晶圆文件夹
    nested_base_path = os.path.join(base_path, 'A-01')
    generate_wafer_data(nested_base_path, 'D', 1)
    
    print("\n所有晶圆数据生成完成，包括嵌套的晶圆文件夹！")