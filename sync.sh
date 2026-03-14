#!/bin/bash

# ====== 配置区域（你只需要改这里） ======
UPSTREAM_REPO="https://github.com/tibbar213/team-manage.git"   # 上游仓库地址
UPSTREAM_BRANCH="main"                                     # 上游分支（main 或 master）
LOCAL_UPSTREAM_BRANCH="upstream"                           # 你本地用来同步的分支
# ========================================

echo "🔍 检查 upstream remote 是否存在..."

# 如果 upstream remote 不存在，则自动添加
if ! git remote | grep -q "upstream"; then
    echo "⚙️  未找到 upstream remote，正在添加..."
    git remote add upstream "$UPSTREAM_REPO"
else
    echo "✅ upstream remote 已存在"
fi

echo ""
echo "📥 Step 1: 切换到本地 upstream 分支"

git checkout "$LOCAL_UPSTREAM_BRANCH" || {
    echo "❌ 本地不存在 $LOCAL_UPSTREAM_BRANCH 分支，正在创建..."
    git checkout -b "$LOCAL_UPSTREAM_BRANCH"
}

echo ""
echo "📡 Step 2: 从上游仓库获取最新代码"

git fetch upstream

echo ""
echo "🔄 Step 3: 合并 upstream/$UPSTREAM_BRANCH 到本地 $LOCAL_UPSTREAM_BRANCH"

git merge "upstream/$UPSTREAM_BRANCH" --no-edit --allow-unrelated-histories

echo ""
echo "📤 Step 4: 推送更新到你的远程仓库"

git push origin "$LOCAL_UPSTREAM_BRANCH"

echo ""
echo "🎉 完成！你的 upstream 分支已同步最新上游代码。"
