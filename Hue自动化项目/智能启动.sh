#!/bin/bash

# Hue查询自动化工具智能启动脚本
# 使用方法: ./智能启动.sh

echo "🚀 Hue查询自动化工具智能启动中..."
echo "=================================="

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "📍 项目根目录: $PROJECT_ROOT"
echo "📍 脚本目录: $SCRIPT_DIR"

# 检查Python环境
echo "📋 检查Python环境..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3未安装，正在安装..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        brew install python3
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        sudo apt-get update && sudo apt-get install -y python3 python3-pip
    else
        echo "❌ 不支持的操作系统，请手动安装Python3"
        exit 1
    fi
else
    echo "✅ Python3已安装: $(python3 --version)"
fi

# 检查依赖包
echo "📦 检查依赖包..."
cd "$SCRIPT_DIR"
if ! python3 -c "import pandas, requests" &> /dev/null; then
    echo "📥 安装依赖包..."
    pip3 install -r requirements.txt
else
    echo "✅ 依赖包已安装"
fi

# 检查配置文件
echo "⚙️  检查配置文件..."
if [ ! -f "hue_config.ini" ]; then
    echo "❌ 配置文件不存在，请先配置 hue_config.ini"
    exit 1
fi

# 智能查找SQL文件
echo "📄 查找财务毛利SQL文件..."
SQL_FILE=""
POSSIBLE_PATHS=(
    "$PROJECT_ROOT/财务/财务毛利.sql"
    "$PROJECT_ROOT/.claude/财务/财务毛利.sql"
    "$PROJECT_ROOT/回收宝/财务毛利.sql"
    "$PROJECT_ROOT/资料/财务毛利.sql"
)

for path in "${POSSIBLE_PATHS[@]}"; do
    if [ -f "$path" ]; then
        SQL_FILE="$path"
        echo "✅ 找到SQL文件: $SQL_FILE"
        break
    fi
done

if [ -z "$SQL_FILE" ]; then
    echo "❌ 未找到财务毛利.sql文件"
    echo "🔍 已检查以下路径:"
    for path in "${POSSIBLE_PATHS[@]}"; do
        echo "   - $path"
    done
    exit 1
fi

# 创建导出目录
echo "📁 创建导出目录..."
mkdir -p exports

# 更新Python脚本中的SQL文件路径
echo "🔧 更新Python脚本中的SQL文件路径..."
ESCAPED_SQL_FILE=$(echo "$SQL_FILE" | sed 's/\//\\\//g')
sed -i.bak "s/sql_file = '.*财务毛利\.sql'/sql_file = '$ESCAPED_SQL_FILE'/" hue_automation.py

# 运行自动化脚本
echo "🚀 启动Hue自动化工具..."
echo "=================================="
python3 hue_automation.py

# 恢复原始文件
echo "🔄 恢复原始文件..."
mv hue_automation.py.bak hue_automation.py 2>/dev/null || true

echo ""
echo "🎉 脚本执行完成！"
echo "📁 请查看 exports 目录下的结果文件"
echo "📖 详细说明请查看: Hue自动化使用说明.md"
echo "🔍 SQL文件位置: $SQL_FILE"




