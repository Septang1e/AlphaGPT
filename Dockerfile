# 使用官方轻量级 Python 镜像
FROM python:3.13-slim-bookworm

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件并安装
# 如果没有，可以直接在这里列出我们已知的包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 为了确保基础包都在
RUN pip install asyncpg pandas loguru ccxt aiohttp

# 将当前项目所有文件复制到容器内
COPY . .

# 保持容器运行，等待我们手动触发脚本
CMD ["tail", "-f", "/dev/null"]