import os
import json
import logging
import requests
import numpy as np
from time import sleep
from requests.auth import HTTPBasicAuth
from os.path import expanduser
from sklearn.tree import DecisionTreeRegressor

# =========================
# 日志配置
# =========================
logging.basicConfig(filename='simulation_lowmem.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 登录
# =========================
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        username, password = json.load(f)
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    resp = sess.post('https://api.worldquantbrain.com/authentication')
    if resp.status_code != 201:
        raise Exception(f"Login failed: {resp.status_code} {resp.text}")
    return sess

sess = sign_in()

# =========================
# 断点续跑
# =========================
PROGRESS_FILE = "progress_lowmem.txt"

def save_progress(index):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists(PROGRESS_FILE):
        try:
            return int(open(PROGRESS_FILE).read().strip())
        except:
            return default_start
    return default_start

# =========================
# 流派与因子
# =========================
flow_factors = {
    'value': ['PE', 'PB', 'ROE'],
    'quality': ['EPS', 'NetProfitMargin'],
    'reversal': ['returns', 'momentum_factor'],
    'volatility': ['volatility_factor', 'beta'],
    'hybrid': ['EPS', 'fnd6_fic']
}

ts_ops = ['ts_rank', 'ts_zscore', 'ts_av_diff']
group_ops = ['group_rank', 'group_zscore', 'group_neutralize']
days_list = [60, 200]
groups = ['market', 'industry', 'subindustry', 'sector']

# =========================
# 生成 alpha 表达式
# =========================
def generate_alpha_expressions():
    alphas = []
    for flow, factors in flow_factors.items():
        if flow != 'hybrid':
            for factor in factors:
                for ts_op in ts_ops:
                    for g_op in group_ops:
                        for d in days_list:
                            for grp in groups:
                                alphas.append(f"{g_op}({ts_op}({factor},{d}),{grp})")
        else:
            for i in range(len(factors)):
                for j in range(i+1, len(factors)):
                    f1, f2 = factors[i], factors[j]
                    for ts_op1 in ts_ops:
                        for ts_op2 in ts_ops:
                            for g_op in group_ops:
                                for d in days_list:
                                    for grp in groups:
                                        alphas.append(f"{g_op}({ts_op1}({f1},{d})*{ts_op2}({f2},{d}),{grp})")
    return alphas

alpha_expressions = generate_alpha_expressions()
logging.info(f"总 alpha 数量: {len(alpha_expressions)}")
print(f"总 alpha 数量: {len(alpha_expressions)}")

# =========================
# 封装 alpha
# =========================
def pack_alpha(expr):
    return {
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
        "regular": expr
    }

alpha_list = [pack_alpha(expr) for expr in alpha_expressions]

# =========================
# 获取历史向量函数
# =========================
def get_alpha_history_vector(alpha, sess, ts_length=100, max_poll=10, poll_sleep=2):
    try:
        resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha)
        if resp.status_code != 201:
            print(f"POST 无 Location: {resp.status_code}, {resp.text}")
            return None

        alpha_id = resp.json()['id']
        url = f"https://api.worldquantbrain.com/simulations/{alpha_id}"

        for _ in range(max_poll):
            r = sess.get(url)
            data = r.json()
            if data.get('status') == 'COMPLETE' and 'daily' in data:
                return np.array(data['daily'][-ts_length:])
            elif data.get('status') != 'COMPLETE':
                sleep(poll_sleep)
        return None
    except Exception as e:
        print(f"Exception get_alpha_history_vector: {e}")
        return None

# =========================
# Guided Sampling + 低相关性滑动窗口
# =========================
sim_vectors = []  # 低相关性计算使用
MAX_SIM_VECTORS = 100  # 只保留最近100个
seed_count = 30
X_train, y_train = [], []

print("开始训练决策树 (Guided Sampling)...")
seed_used = 0
for alpha in alpha_list:
    if seed_used >= seed_count:
        break
    vec = get_alpha_history_vector(alpha, sess)
    if vec is None:
        continue
    sim_vectors.append(vec)
    if len(sim_vectors) > MAX_SIM_VECTORS:
        sim_vectors.pop(0)
    feat = [1 if op in alpha['regular'] else 0 for op in ts_ops + group_ops]
    X_train.append(feat)
    y_train.append(vec.mean())
    seed_used += 1
    print(f"[seed {seed_used}] 成功获取历史向量")

if X_train:
    tree = DecisionTreeRegressor(max_depth=5)
    tree.fit(X_train, y_train)
    print("决策树训练完成")
else:
    tree = None
    print("没有训练数据，跳过 Guided Sampling")

# =========================
# 顺序提交 alpha
# =========================
alpha_fail_attempt_tolerance = 15
start_index = load_progress(0)
print(f"== 从 index {start_index} 继续运行 ==")
logging.info(f"== 从 index {start_index} 继续运行 ==")

for index in range(start_index, len(alpha_list)):
    alpha = alpha_list[index]

    print(f"[{index}] 提交 alpha: {alpha['regular']}, 等待历史向量...")
    keep_trying = True
    fail_count = 0
    vec = None

    while keep_trying:
        vec = get_alpha_history_vector(alpha, sess)
        if vec is not None:
            keep_trying = False
            save_progress(index + 1)
        else:
            fail_count += 1
            print(f"[{index}] 未获取历史向量，sleep 5s 重试... (fail_count={fail_count})")
            sleep(5)
            if fail_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()
                fail_count = 0
                keep_trying = False

    if vec is None:
        print(f"[{index}] 获取历史向量失败，跳过")
        continue

    # 低相关性筛选
    if sim_vectors:
        corr_max = max([np.corrcoef(vec, v)[0,1] for v in sim_vectors])
        if corr_max > 0.7:
            print(f"[{index}] alpha 与已有 alpha 相关性 {corr_max:.2f}，跳过")
            continue

    # 保存新 alpha 向量到滑动窗口
    sim_vectors.append(vec)
    if len(sim_vectors) > MAX_SIM_VECTORS:
        sim_vectors.pop(0)

    logging.info(f"新 alpha: {alpha['regular']}")
    print(f"[{index}] 新 alpha: {alpha['regular']}")
