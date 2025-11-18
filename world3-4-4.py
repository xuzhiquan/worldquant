# 登录
import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth

#1029
def sign_in():
    # Load credentials # 加载凭证
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    username, password = credentials

    # Create a session object # 创建会话对象
    sess = requests.Session()

    # Set up basic authentication # 设置基本身份验证
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication # 向API发送POST请求进行身份验证
    response = sess.post('https://api.worldquantbrain.com/authentication')

    # Print response status and content for debugging # 打印响应状态和内容以调试
    print(response.status_code)
    print(response.json())
    return sess


sess = sign_in()

# -------------------------------------------------------------------------
# 获取数据集ID为fundamental6（Company Fundamental Data for Equity）下的所有数据字段
# -------------------------------------------------------------------------
def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
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


# 定义搜索范围
searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
# 从数据集中获取数据字段
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6')
# 过滤类型为 "MATRIX" 的数据字段
fnd6 = fnd6[fnd6['type'] == "MATRIX"]
# 提取数据字段的ID并转换为列表
datafields_list_fnd6 = fnd6['id'].values
print(datafields_list_fnd6)
print(len(datafields_list_fnd6))


# ======================================================================
# 重新定义 alpha_expressions：使用 IV-difference Alpha 模板生成器
# ======================================================================
import random

vol_call_pool = [
    "implied_volatility_call_60",
    "implied_volatility_call_120",
    "implied_volatility_call_180"
]

vol_put_pool = [
    "implied_volatility_put_60",
    "implied_volatility_put_120",
    "implied_volatility_put_180"
]

hist_vol_pool = [
    "historical_volatility_20",
    "historical_volatility_60",
    "historical_volatility_120",
    "historical_volatility_250"
]

bucket_ranges = [
    '"0.1,1,0.1"',
    '"0.05,1,0.05"',
    '"0.2,1,0.2"',
]

alpha_expressions = []

# 生成 3000 个 alpha，你可以按需改为更多
for i in range(3000):
    call = random.choice(vol_call_pool)
    put = random.choice(vol_put_pool)
    hv  = random.choice(hist_vol_pool)
    br  = random.choice(bucket_ranges)

    iv_diff = f"({call}-{put})"
    std_group = f"bucket(rank({hv}), range={br})"
    expr = f"group_neutralize({iv_diff}, {std_group})"

    alpha_expressions.append(expr)

print(f"there are total {len(alpha_expressions)} alpha expressions")
print(alpha_expressions[:5])


# -------------------------------------------------------------------------
# 将 alpha 封装成 simulation JSON（完全保留你的原逻辑）
# -------------------------------------------------------------------------
alpha_list = []

print("将alpha表达式与setting封装")
for index, alpha_expression in enumerate(alpha_expressions, start=1):
    print(f"正在循环第 {index} 个元素, 组装alpha表达式: {alpha_expression}")
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


# -------------------------------------------------------------------------
# 保持你原来的：逐个 POST，断点续跑 + 自动重连 + 容错逻辑
# -------------------------------------------------------------------------
import logging
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from time import sleep

alpha_fail_attempt_tolerance = 15

for index in range(0, len(alpha_list)):
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

            sim_progress_url = sim_resp.headers['Location']
            logging.info(f'Alpha location is: {sim_progress_url}')
            print(f'Alpha location is: {sim_progress_url}')

            keep_trying = False

        except Exception as e:
            logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
            print("No Location, sleep 15 and retry")
            sleep(15)
            failure_count += 1

            if failure_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()
                failure_count = 0
                logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")
                print(f"No location for too many times, move to next alpha {alpha['regular']}")
                break
