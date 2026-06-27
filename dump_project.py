#!/usr/bin/env python3
import os
import sys

# ==================== 配置区域 ====================
# 输出的文件名称
OUTPUT_FILE = "project_context_snapshot.txt"

# 需要忽略的文件夹（防止膨胀和死循环）
IGNORE_DIRS = {
    ".git", ".github", "__pycache__", ".pytest_cache", 
    "venv", ".venv", "env", "build", "dist", ".idea", ".vscode"
}

# 需要忽略的文件扩展名（排除权重、音视频、日志、编译文件）
IGNORE_EXTS = {
    ".pyc", ".pyo", ".pyd", ".pt", ".pth", ".ckpt", ".bin", 
    ".h5", ".onnx", ".db", ".sqlite", ".log", ".png", ".jpg", 
    ".jpeg", ".gif", ".ico", ".pstat", ".exe", ".dll", ".so"
}

# 明确包含的文件名（即使没有后缀或在忽略名单外，如配置文件）
INCLUDE_FILES = {
    "requirements.txt", "Dockerfile", ".env.example", "README.md"
}
# ==================================================

def generate_tree(dir_path, prefix=""):
    """生成项目的树状图结构"""
    tree_str = ""
    try:
        entries = sorted(os.listdir(dir_path))
    except OSError:
        return tree_str

    # 过滤 entries
    visible_entries = []
    for entry in entries:
        full_path = os.path.join(dir_path, entry)
        if os.path.isdir(full_path):
            if entry in IGNORE_DIRS:
                continue
        else:
            _, ext = os.path.splitext(entry)
            if ext in IGNORE_EXTS and entry not in INCLUDE_FILES:
                continue
        visible_entries.append(entry)

    count = len(visible_entries)
    for i, entry in enumerate(visible_entries):
        is_last = (i == count - 1)
        connector = "└── " if is_last else "├── "
        tree_str += f"{prefix}{connector}{entry}\n"
        
        full_path = os.path.join(dir_path, entry)
        if os.path.isdir(full_path):
            next_prefix = prefix + ("    " if is_last else "│   ")
            tree_str += generate_tree(full_path, next_prefix)
            
    return tree_str

def dump_project():
    project_root = os.path.abspath(os.path.dirname(__file__))
    script_name = os.path.basename(__file__)
    
    print(f"🚀 开始扫描项目根目录: {project_root}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        # 1. 写入头部元数据说明
        out.write("==================================================\n")
        out.write("               PROJECT SNAPSHOT SUMMARY           \n")
        out.write("==================================================\n")
        out.write(f"Project Root: {project_root}\n")
        out.write("Generated automatically for LLM analysis.\n\n")
        
        # 2. 写入项目目录树状图，方便大模型宏观建立 DDD 模块索引
        out.write("## 1. PROJECT STRUCTURE TREE\n")
        out.write("```text\n")
        out.write(".\n" + generate_tree(project_root))
        out.write("```\n\n")
        
        out.write("## 2. SOURCE CODE DETAILS\n")
        out.write("Below is the complete content of all verified source files.\n\n")
        
        file_count = 0
        
        # 3. 遍历并合并文件
        for root, dirs, files in os.walk(project_root):
            # 过滤不需要的文件夹（原地修改以跳过子目录）
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for file in sorted(files):
                # 排除脚本自身和输出文件
                if file in (script_name, OUTPUT_FILE):
                    continue
                    
                _, ext = os.path.splitext(file)
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, project_root)
                
                # 过滤规则校验
                if ext in IGNORE_EXTS and file not in INCLUDE_FILES:
                    continue
                    
                print(f" -> 正在解析: {rel_path}")
                file_count += 1
                
                # 写入文件边界与路径标记（使用 Markdown 语法进行语义包裹）
                out.write(f"### FILE: {file}\n")
                out.write(f"**Path:** `{rel_path}`\n")
                
                # 根据后缀决定 Markdown 代码块的语法高亮类型
                lang = "python" if ext == ".py" else ("json" if ext == ".json" else "text")
                out.write(f"```{lang}\n")
                
                try:
                    with open(full_path, "r", encoding="utf-8", errors="replace") as f:
                        out.write(f.read())
                except Exception as e:
                    out.write(f"[ERROR READING FILE: {str(e)}]\n")
                    
                out.write("\n```\n")
                out.write("-" * 60 + "\n\n")
                
        print(f"导出完成！共打包 {file_count} 个源码文件。")
        print(f"输出结果已保存至: {os.path.join(project_root, OUTPUT_FILE)}")

if __name__ == "__main__":
    dump_project()