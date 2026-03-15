"""
业务常量定义
"""

# 兑换码渠道列表
REDEMPTION_CHANNELS = [
    {"value": "xianyu",     "label": "闲鱼"},
    {"value": "siyou",      "label": "私域"},
    {"value": "faka",       "label": "发卡"},
    {"value": "ldo",        "label": "LDO"},
    {"value": "ziyong",     "label": "自用"},
    {"value": "hezuoshang", "label": "合作商"},
]

# 渠道 value -> label 映射，方便快速查找
CHANNEL_LABEL_MAP = {ch["value"]: ch["label"] for ch in REDEMPTION_CHANNELS}
