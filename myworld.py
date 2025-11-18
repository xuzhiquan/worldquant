import os
import json
import logging
from time import sleep
import requests
from requests.auth import HTTPBasicAuth
from os.path import expanduser

# =========================
# 日志配置
# =========================
logging.basicConfig(filename='simulation2.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 登录函数
# =========================
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)
    username, password = credentials
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    response = sess.post('https://api.worldquantbrain.com/authentication')
    if response.status_code !=  200 and response.status_code!= 201:
        raise Exception(f"Login failed: {response.status_code}, {response.text}")
    return sess

# =========================
# 断点续跑函数
# =========================
def save_progress(index):
    with open("progress2.txt", "w") as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists("progress2.txt"):
        try:
            return int(open("progress2.txt").read().strip())
        except:
            return default_start
    return default_start

# =========================
# 高质量 alpha 生成器（生成即提交）
# =========================
def generate_and_submit_alphas(basic_factors,
                               sess,
                               ts_ops=None,
                               group_ops=None,
                               days_list=None,
                               groups=None,
                               instrumentType='EQUITY',
                               region='USA',
                               universe='TOP3000',
                               delay=1,
                               alpha_fail_attempt_tolerance=15):
    if ts_ops is None:
        ts_ops = ['ts_rank','ts_zscore','ts_av_diff']
    if group_ops is None:
        group_ops = ['group_rank','group_zscore','group_neutralize']
    if days_list is None:
        days_list = [5, 10, 20, 60, 200]
    if groups is None:
        groups = ['market','industry','subindustry','sector']

    start_index = load_progress(default_start=0)
    logging.info(f"== 从 index {start_index} 开始 ==")
    print(f"== 从 index {start_index} 开始 ==")

    index = 0
    for g_op in group_ops:
        for ts_op in ts_ops:
            for factor in basic_factors:
                for d in days_list:
                    for grp in groups:
                        if index < start_index:
                            index += 1
                            continue

                        alpha_expr = f"{g_op}({ts_op}({factor}, {d}), {grp})"
                        alpha_json = {
                            "type": "REGULAR",
                            "settings": {
                                "instrumentType": instrumentType,
                                "region": region,
                                "universe": universe,
                                "delay": delay,
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
                        }

                        keep_trying = True
                        failure_count = 0
                        while keep_trying:
                            try:
                                resp = sess.post(
                                    'https://api.worldquantbrain.com/simulations',
                                    json=alpha_json
                                )
                                if 'Location' not in resp.headers:
                                    raise Exception("No Location header")
                                logging.info(f"{index}: {alpha_expr}, Location: {resp.headers['Location']}")
                                print(f"{index}: {alpha_expr}, Location: {resp.headers['Location']}")
                                keep_trying = False
                                save_progress(index + 1)
                            except Exception as e:
                                failure_count += 1
                                logging.error(f"Error submitting alpha: {e}, retrying...")
                                print(f"Error submitting alpha: {e}, sleeping 15s...")
                                sleep(15)
                                if failure_count >= alpha_fail_attempt_tolerance:
                                    logging.error(f"Exceeded retry limit, re-login & skip alpha: {alpha_expr}")
                                    print(f"Exceeded retry limit, re-login & skip alpha: {alpha_expr}")
                                    sess = sign_in()
                                    failure_count = 0
                                    save_progress(index + 1)
                                    break

                        index += 1

# =========================
# 示例用法
# =========================
basic_factors = ['ROE','PE','EPS','NetProfitMargin']

# 登录
sess = sign_in()

# 生成并提交 alpha
generate_and_submit_alphas(basic_factors, sess)
