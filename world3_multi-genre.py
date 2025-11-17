import os
import json
import logging
import requests
import numpy as np
from time import sleep, time
from requests.auth import HTTPBasicAuth
from os.path import expanduser
from concurrent.futures import ThreadPoolExecutor
from sklearn.tree import DecisionTreeRegressor

# =========================
# 日志
# =========================
logging.basicConfig(filename='simulation_3.log', level=logging.INFO,
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
    if resp.status_code != 200 and resp.status_code !=201:
        raise Exception(f"Login failed: {resp.status_code} {resp.text}")
    return sess

sess = sign_in()

# =========================
# 断点续跑
# =========================
PROGRESS_FILE = "progress3.txt"
RESULT_FILE = "generated_alphas_3.txt"

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
# 流派、因子、操作符
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

# 封装 alpha
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
# 获取真实历史向量
# =========================
def get_alpha_history_vector(alpha, sess, ts_length=100):
    try:
        resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha)
        if 'Location' not in resp.headers:
            return None
        url = resp.headers['Location']
        for _ in range(40):
            r = sess.get(url + "/poll")
            data = r.json()
            if 'daily' in data:
                return np.array(data['daily'][-ts_length:])
            sleep(3)
    except:
        return None
    return None

# =========================
# 种子采样训练决策树
# =========================
seed_count = 50
X_train, y_train = [], []
sim_vectors = []

for alpha in alpha_list[:seed_count]:
    vec = get_alpha_history_vector(alpha, sess)
    if vec is None:
        continue
    sim_vectors.append(vec)
    feat = [0]*(len(ts_ops)+len(group_ops))
    for i, op in enumerate(ts_ops+group_ops):
        if op in alpha['regular']:
            feat[i] = 1
    X_train.append(feat)
    y_train.append(vec.mean())

tree = DecisionTreeRegressor(max_depth=5)
tree.fit(X_train, y_train)

# =========================
# Guided sampling + 并发提交 + 低相关性筛选
# =========================
target_count = 5000
final_alpha_list = []
start_index = load_progress(0)
alpha_fail_attempt_tolerance = 10
early_stop_seconds = 3600
last_new_time = time()
ts_length = 100

def submit_alpha(alpha):
    keep_trying, fail_count = True, 0
    while keep_trying:
        vec = get_alpha_history_vector(alpha, sess, ts_length)
        if vec is not None:
            return alpha['regular'], vec
        fail_count += 1
        sleep(5)
        if fail_count >= alpha_fail_attempt_tolerance:
            return None, None

from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=3)

index = start_index
while len(final_alpha_list) < target_count and index < len(alpha_list):
    alpha = alpha_list[index]
    # 决策树评分
    feat = [0]*(len(ts_ops)+len(group_ops))
    for i, op in enumerate(ts_ops+group_ops):
        if op in alpha['regular']:
            feat[i] = 1
    score = tree.predict([feat])[0]
    if score < np.percentile(y_train, 20):  # 低分跳过
        index += 1
        continue

    # 并发提交 alpha
    alpha_expr, vec = executor.submit(submit_alpha, alpha).result()
    if vec is None:
        index += 1
        continue

    # 低相关性筛选
    if sim_vectors:
        corr_max = max([np.corrcoef(vec, v)[0,1] for v in sim_vectors])
        if corr_max > 0.7:
            index += 1
            continue

    sim_vectors.append(vec)
    final_alpha_list.append(alpha)
    last_new_time = time()
    logging.info(f"{len(final_alpha_list)}: {alpha_expr}")
    print(f"{len(final_alpha_list)}: {alpha_expr}")
    save_progress(index)
    index += 1

    if time() - last_new_time > early_stop_seconds:
        logging.info("早停触发，长时间未找到新 alpha")
        break

# =========================
# 保存结果
# =========================
with open(RESULT_FILE, "w") as f:
    json.dump([a['regular'] for a in final_alpha_list], f)

logging.info(f"生成 alpha 完成，总数量: {len(final_alpha_list)}")
print(f"生成 alpha 完成，总数量: {len(final_alpha_list)}")
