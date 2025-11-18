# 登录
import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth

#1029
def sign_in():
    # Load credentials
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password
    username, password = credentials

    # Create a session object
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)

    # Authentication
    response = sess.post('https://api.worldquantbrain.com/authentication')
    print(response.status_code)
    print(response.json())
    return sess

sess = sign_in()


# ======================================================
# 获取数据字段（原样保留）
# ======================================================
def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    import pandas as pd
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']

    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6')

fnd6 = fnd6[fnd6['type'] == "MATRIX"]
datafields_list_fnd6 = fnd6['id'].values
print(datafields_list_fnd6)
print(len(datafields_list_fnd6))


# ======================================================
# 构造 alpha 表达式（原样保留）
# ======================================================
group_compare_op = ['group_rank', 'group_zscore', 'group_neutralize']
ts_compare_op = ['ts_rank', 'ts_zscore', 'ts_av_diff']
company_fundamentals = datafields_list_fnd6
days = [60, 200]
group = ['market', 'industry', 'subindustry', 'sector', 'densify(pv13_h_f1_sector)']

alpha_expressions = []
for gco in group_compare_op:
    for tco in ts_compare_op:
        for cf in company_fundamentals:
            for d in days:
                for grp in group:
                    alpha_expressions.append(f"{gco}({tco}({cf}, {d}), {grp})")

print(f"there are total {len(alpha_expressions)} alpha expressions")
print(alpha_expressions[:5])
print(len(alpha_expressions))


# ======================================================
# 封装 alpha（原样保留）
# ======================================================
alpha_list = []
print("将alpha表达式与setting封装")

for index, alpha_expression in enumerate(alpha_expressions, start=1):
    print(f"正在循环第 {index} 个元素,组装alpha表达式: {alpha_expression}")
    simulation_data = {
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
        "regular": alpha_expression
    }
    alpha_list.append(simulation_data)

print(f"there are {len(alpha_list)} Alphas to simulate")
print(alpha_list[0])


# ======================================================
# 日志配置（原样保留）
# ======================================================
import logging
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# ======================================================
# ★★★ 断点续跑功能（新增，仅此处有改动） ★★★
# ======================================================
import os

def save_progress(index):
    with open("progress.txt", "w") as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists("progress.txt"):
        try:
            with open("progress.txt", "r") as f:
                return int(f.read().strip())
        except:
            return default_start
    return default_start


# ======================================================
# 回测（仅加入断点续跑，其余全部原样）
# ======================================================
from time import sleep

alpha_fail_attempt_tolerance = 15

start_index = load_progress(default_start=0)
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

            # ★ 成功后立即保存断点
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

                # 跳过也要保存断点
                save_progress(index + 1)
                break
