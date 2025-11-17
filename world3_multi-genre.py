import os
import json
import logging
import requests
import numpy as np
from time import sleep
from requests.auth import HTTPBasicAuth
from os.path import expanduser

# =========================
# 日志
# =========================
logging.basicConfig(filename='simulation_flow.log', level=logging.INFO,
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
PROGRESS_FILE = "progress_flow.txt"

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
    'hybrid': ['EPS', 'fnd6_fic']  # hybrid 流派用因子组合
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
            # hybrid 流派组合因子
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
# 回测 + 断点续跑
# =========================
alpha_fail_attempt_tolerance = 15
start_index = load_progress(0)
print(f"== 从 index {start_index} 继续运行 ==")
logging.info(f"== 从 index {start_index} 继续运行 ==")

for index in range(start_index, len(alpha_list)):
    alpha = alpha_list[index]
    print(f"{index}: {alpha['regular']}")
    logging.info(f"{index}: {alpha['regular']}")

    keep_trying = True
    failure_count = 0

    while keep_trying:
        try:
            sim_resp = sess.post(
                'https://api.worldquantbrain.com/simulations',
                json=alpha
            )

            if 'Location' not in sim_resp.headers:
                raise Exception("No Location header found")

            sim_progress_url = sim_resp.headers['Location']
            logging.info(f'Alpha location is: {sim_progress_url}')
            print(f'Alpha location is: {sim_progress_url}')

            keep_trying = False
            save_progress(index + 1)

        except Exception as e:
            failure_count += 1
            logging.error(f"Error: {e}. No Location, retry after sleep.")
            print("No Location, sleeping 15s and retry...")
            sleep(15)

            if failure_count >= alpha_fail_attempt_tolerance:
                logging.error(f"Exceeded retry limit, re-login & skip alpha: {alpha['regular']}")
                print(f"Exceeded retry limit, re-login & skip alpha: {alpha['regular']}")
                sess = sign_in()
                failure_count = 0
                save_progress(index + 1)
                break
