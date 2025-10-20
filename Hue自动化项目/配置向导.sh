#!/bin/bash

# Hue配置向导脚本
# 使用方法: ./配置向导.sh

echo "🔧 Hue配置向导启动中..."
echo "=========================="

CONFIG_FILE="hue_config.ini"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ 配置文件不存在，正在创建..."
    touch "$CONFIG_FILE"
fi

echo ""
echo "📝 请按照提示输入Hue连接信息："
echo ""

# 获取Hue服务器地址
echo "🌐 Hue服务器地址"
echo "   当前值: $(grep '^base_url' "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || echo 'http://119.23.30.106:8889')"
read -p "   请输入新的地址 (直接回车使用默认值): " BASE_URL
BASE_URL=${BASE_URL:-"http://119.23.30.106:8889"}

# 获取用户名
echo ""
echo "👤 Hue用户名"
echo "   当前值: $(grep '^username' "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || echo 'your_username')"
read -p "   请输入用户名: " USERNAME
if [ -z "$USERNAME" ]; then
    echo "❌ 用户名不能为空！"
    exit 1
fi

# 获取密码
echo ""
echo "🔒 Hue密码"
read -s -p "   请输入密码: " PASSWORD
echo ""
if [ -z "$PASSWORD" ]; then
    echo "❌ 密码不能为空！"
    exit 1
fi

# 获取查询超时时间
echo ""
echo "⏱️  查询超时时间（秒）"
echo "   当前值: $(grep '^timeout' "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || echo '300')"
read -p "   请输入超时时间 (直接回车使用默认值): " TIMEOUT
TIMEOUT=${TIMEOUT:-"300"}

# 获取数据库类型
echo ""
echo "🗄️  数据库类型"
echo "   当前值: $(grep '^database_type' "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || echo 'hive')"
echo "   可选值: hive, impala, mysql, postgresql"
read -p "   请输入数据库类型 (直接回车使用默认值): " DB_TYPE
DB_TYPE=${DB_TYPE:-"hive"}

echo ""
echo "📝 正在更新配置文件..."

# 创建或更新配置文件
cat > "$CONFIG_FILE" << EOF
[hue]
# Hue服务器地址
base_url = ${BASE_URL}
# 你的Hue用户名
username = ${USERNAME}
# 你的Hue密码
password = ${PASSWORD}

[query]
# 查询超时时间（秒）
timeout = ${TIMEOUT}
# 最大重试次数
max_retries = 3
# 批处理大小
batch_size = 10000
# 数据库类型
database_type = ${DB_TYPE}

[export]
# 导出目录
directory = ./exports
# 默认导出格式
format = csv
# 文件编码
encoding = utf-8

[optimization]
# 启用分区裁剪
enable_partition_pruning = true
# 启用列裁剪
enable_column_pruning = true
# 最大内存使用
max_memory = 2GB
# 启用查询缓存
enable_cache = true
EOF

echo "✅ 配置文件更新完成！"
echo ""

# 显示配置摘要
echo "📋 配置摘要："
echo "   🌐 服务器地址: $BASE_URL"
echo "   👤 用户名: $USERNAME"
echo "   ⏱️  超时时间: ${TIMEOUT}秒"
echo "   🗄️  数据库类型: $DB_TYPE"
echo ""

# 测试连接
echo "🔍 测试Hue连接..."
if curl -s -I "$BASE_URL" > /dev/null; then
    echo "✅ Hue服务器连接正常"
else
    echo "❌ 无法连接到Hue服务器，请检查地址是否正确"
fi

echo ""
echo "🎉 配置完成！现在你可以运行："
echo "   ./智能启动.sh"
echo ""
echo "💡 提示：如果配置有误，可以重新运行此脚本修改"




