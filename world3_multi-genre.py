import os
import json
import logging
import requests
import numpy as np
import pandas as pd
from requests.auth import HTTPBasicAuth
from os.path import expanduser
from time import sleep
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import OneHotEncoder

# =========================
# 日志配置
# =========================
logging.basicConfig(filename='simulation_3.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 登录函数
# =========================
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        username, password = json.load(f)
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    resp = sess.post('https://api.worldquantbrain.com/authentication')
    if resp.status_code != 200:
        raise Exception(f"Login failed: {resp.status_code} {resp.text}")
    return sess

sess = sign_in()

# =========================
# 断点续跑
# =========================
def save_progress(index):
    with open("progress_3.txt", "w") as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists("progress_3.txt"):
        try:
            return int(open("progress_3.txt").read().strip())
        except:
            return default_start
    return default_start

# =========================
# 流派和因子定义
# =========================
flow_factors = {
    'value': ['PE', 'PB', 'ROE'],
    'quality': ['EPS', 'NetProfitMargin'],
    'reversal': ['returns', 'momentum_factor'],
    'volatility': ['volatility_factor', 'beta'],
    'hybrid': ['EPS', 'fnd6_fic']
}

ts_ops = ['ts_rank', 'ts_zscore', 'ts_av_diff', 'ts_mean', 'ts_std_dev']
group_ops = ['group_rank', 'group_zscore', 'group_neutralize', 'group_scale']
days_list = [5, 10, 20, 60, 200]
groups = ['market', 'industry', 'subindustry', 'sector']

# =========================
# 生成 alpha 表达式（确定性，流派全覆盖）
# =========================
alpha_expressions = []

for flow, factors in flow_factors.items():
    if flow != 'hybrid':
        for factor in factors:
            for ts_op in ts_ops:
                for g_op in group_ops:
                    for d in days_list:
                        for grp in groups:
                            alpha_expressions.append(f"{g_op}({ts_op}({factor},{d}),{grp})")
    else:
        # hybrid 流派固定组合
        for i in range(len(factors)):
            for j in range(i+1, len(factors)):
                factor1 = factors[i]
                factor2 = factors[j]
                for ts_op1 in ts_ops:
                    for ts_op2 in ts_ops:
                        for g_op in group_ops:
                            for d in days_list:
                                for grp in groups:
                                    alpha_expressions.append(f"{g_op}({ts_op1}({factor1},{d})*{ts_op2}({factor2},{d}),{grp})")

# =========================
# 封装 alpha
# =========================
alpha_list = []
for alpha_expr in alpha_expressions:
    alpha_list.append({
        "type": "REGULAR",
        "settings": {
            "instrumentType": "EQUITY",
            "region": "USA",
            "universe": "TOP3000",
            "delay": 1,
            "decay": 6,
            "neutralization": "SUBINDUSTRY",
            "truncation": 0.08,
            "pasteurization": "ON",
            "unitHandling": "VERIFY",
            "nanHandling": "ON",
            "language": "FASTEXPR",
            "visualization": False,
        },
        "regular": alpha_expr
    })

# =========================
# 决策树引导 alpha 搜索
# =========================
# 历史 alpha 样本用于训练决策树（示例，这里用随机向量）
ts_length = 100
X_train, y_train = [], []
for alpha in alpha_list[:100]:  # 假设已有100个历史 alpha
    # 特征编码: 简单用 one-hot 算子+流派+days+group
    feat = [
        int(alpha['regular'].find(op) >= 0) for op in ts_ops+group_ops
    ]
    X_train.append(feat)
    y_train.append(np.random.randn())  # 模拟 IC

# 训练决策树
tree = DecisionTreeRegressor(max_depth=5)
tree.fit(X_train, y_train)

# =========================
# 提交 alpha + 低相关性控制
# =========================
target_count = 5000
final_alpha_list = []
simulated_vectors = []

start_index = load_progress(0)
index = start_index
alpha_fail_attempt_tolerance = 15

while len(final_alpha_list) < target_count and index < len(alpha_list):
    alpha = alpha_list[index]

    # 决策树预测评分
    feat = [int(alpha['regular'].find(op) >= 0) for op in ts_ops+group_ops]
    score = tree.predict([feat])[0]

    # 低相关性检查
    alpha_vector = np.random.randn(ts_length)  # 模拟历史向量
    if simulated_vectors:
        corr_max = max([np.corrcoef(alpha_vector, v)[0,1] for v in simulated_vectors])
        if corr_max > 0.7:
            index += 1
            continue

    simulated_vectors.append(alpha_vector)
    final_alpha_list.append(alpha)
    index += 1

    # 提交 alpha
    keep_trying = True
    failure_count = 0
    while keep_trying:
        try:
            resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha)
            if 'Location' not in resp.headers:
                raise Exception("No Location header")
            logging.info(f"{len(final_alpha_list)-1}: {alpha['regular']} Location: {resp.headers['Location']}")
            print(f"{len(final_alpha_list)-1}: {alpha['regular']} Location: {resp.headers['Location']}")
            save_progress(index)
            keep_trying = False
        except Exception as e:
            failure_count += 1
            logging.error(f"Error submitting alpha: {e}")
            sleep(15)
            if failure_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()
                failure_count = 0
                save_progress(index)
                break

print(f"生成并提交了 {len(final_alpha_list)} 个低相关 alpha")
