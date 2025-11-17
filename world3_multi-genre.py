import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth
import pandas as pd
import logging
import os
from time import sleep
import random
import numpy as np

# =========================
# 日志配置 添加說明
# =========================
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 登录函数
# =========================
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        username, password = json.load(f)
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    response = sess.post('https://api.worldquantbrain.com/authentication')
    print(response.status_code, response.json())
    return sess

sess = sign_in()

# =========================
# 获取数据字段
# =========================
def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']

    if len(search) == 0:
        url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}&dataset.id={dataset_id}&limit=50&offset={{x}}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}&limit=50&search={search}&offset={{x}}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    df = pd.DataFrame(datafields_list_flat)
    df = df[df['type'] == "MATRIX"]
    # 转成 list，防止 random.sample 出错
    return list(df['id'].values)

searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
company_fundamentals = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6')

# =========================
# 随机 alpha 生成函数
# =========================
def generate_random_alpha(factors):
    value_factors = ['PE', 'PB', 'ROE', 'NetProfitMargin']
    quality_factors = ['ROE', 'GrossMargin', 'NetProfitMargin', 'DebtToEquity']
    reversal_factors = ['Close', 'ROE', 'EPS']
    volatility_factors = ['Close', 'NetProfitMargin']

    ts_ops = ['ts_rank', 'ts_zscore', 'ts_av_diff', 'ts_mean', 'ts_std_dev', 'ts_delta', 'ts_delay']
    group_ops = ['group_rank', 'group_zscore', 'group_neutralize', 'group_scale']
    math_ops = ['abs', 'multiply', 'add', 'subtract', 'power']

    days = [5, 10, 20, 60, 200]
    groups = ['market', 'industry', 'subindustry', 'sector']

    flow = random.choice(['value', 'quality', 'reversal', 'volatility', 'hybrid'])

    if flow == 'hybrid':
        factor1, factor2 = random.sample(factors, 2)
        ts_op1, ts_op2 = random.sample(ts_ops, 2)
        g_op = random.choice(group_ops)
        d1, d2 = random.sample(days, 2)
        group_field = random.choice(groups)
        return f"{g_op}({ts_op1}({factor1},{d1}) + {ts_op2}({factor2},{d2}), {group_field})"

    # 单流派
    if flow == 'value':
        factor = random.choice(value_factors)
        ts_op = random.choice(ts_ops[:4])
    elif flow == 'quality':
        factor = random.choice(quality_factors)
        ts_op = random.choice(ts_ops[:5])
    elif flow == 'reversal':
        factor = random.choice(reversal_factors)
        ts_op = random.choice(['ts_delta', 'ts_rank', 'ts_zscore'])
    elif flow == 'volatility':
        factor = random.choice(volatility_factors)
        ts_op = random.choice(['ts_std_dev', 'ts_zscore'])

    g_op = random.choice(group_ops)
    day = random.choice(days)
    group_field = random.choice(groups)

    math_choice = random.choice([None] + math_ops)
    if math_choice == 'abs':
        expr = f"{g_op}({ts_op}(abs({factor}), {day}), {group_field})"
    elif math_choice == 'multiply':
        factor2 = random.choice(factors)
        expr = f"{g_op}({ts_op}({factor},{day})*{ts_op}({factor2},{day}), {group_field})"
    elif math_choice == 'add':
        factor2 = random.choice(factors)
        expr = f"{g_op}({ts_op}({factor},{day})+{ts_op}({factor2},{day}), {group_field})"
    elif math_choice == 'subtract':
        factor2 = random.choice(factors)
        expr = f"{g_op}({ts_op}({factor},{day})-{ts_op}({factor2},{day}), {group_field})"
    elif math_choice == 'power':
        expr = f"{g_op}({ts_op}({factor},{day})^2, {group_field})"
    else:
        expr = f"{g_op}({ts_op}({factor},{day}), {group_field})"
    return expr

# =========================
# 断点续跑
# =========================
def save_progress(index):
    with open("progress3.txt", "w") as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists("progress3.txt"):
        try:
            with open("progress3.txt","r") as f:
                return int(f.read().strip())
        except:
            return default_start
    return default_start

# =========================
# 生成低相关 alpha 并提交
# =========================
target_count = 5000
final_alpha_list = []
historical_vectors = []
ts_length = 1000  # 假设历史长度

alpha_fail_attempt_tolerance = 15
start_index = load_progress(0)
print(f"== 从 index {start_index} 继续运行 ==")
logging.info(f"== 从 index {start_index} 继续运行 ==")

index = start_index
while len(final_alpha_list) < target_count:
    alpha_expr = generate_random_alpha(company_fundamentals)
    
    # 用随机历史向量模拟 alpha 值
    alpha_vector = np.random.randn(ts_length)
    
    # 相关性检查
    if len(historical_vectors) == 0:
        final_alpha_list.append(alpha_expr)
        historical_vectors.append(alpha_vector)
    else:
        max_corr = max([np.corrcoef(alpha_vector, vec)[0,1] for vec in historical_vectors])
        if max_corr < 0.7:
            final_alpha_list.append(alpha_expr)
            historical_vectors.append(alpha_vector)
    
    # 封装 alpha 数据
    alpha_data = {
        "type":"REGULAR",
        "settings":{
            "instrumentType":"EQUITY",
            "region":"USA",
            "universe":"TOP3000",
            "delay":1,
            "decay":6,
            "neutralization":"SUBINDUSTRY",
            "truncation":0.08,
            "pasteurization":"ON",
            "unitHandling":"VERIFY",
            "nanHandling":"ON",
            "language":"FASTEXPR",
            "visualization":False,
        },
        "regular": alpha_expr
    }
    
    # 提交 alpha
    keep_trying = True
    failure_count = 0
    while keep_trying:
        try:
            sim_resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha_data)
            if 'Location' not in sim_resp.headers:
                raise Exception("No Location header")
            logging.info(f"{index}: {alpha_expr} Location: {sim_resp.headers['Location']}")
            print(f"{index}: {alpha_expr} Location: {sim_resp.headers['Location']}")
            keep_trying = False
            save_progress(index+1)
        except Exception as e:
            failure_count += 1
            logging.error(f"Error: {e}. retry after sleep.")
            sleep(15)
            if failure_count >= alpha_fail_attempt_tolerance:
                logging.error(f"Exceeded retry limit, re-login & skip {alpha_expr}")
                sess = sign_in()
                failure_count = 0
                save_progress(index+1)
                break
    index += 1
