import random
import logging
import os
from time import sleep
from collections import deque
from requests.auth import HTTPBasicAuth
import requests
import json

# =========================
# 配置
# =========================
POP_SIZE = 50
GENERATIONS = 20
CROSSOVER_RATE = 0.5
MUTATION_RATE = 0.3
SIMILARITY_THRESHOLD = 0.7
FAILURE_TOLERANCE = 15
PROGRESS_FILE = "progress_GA.txt"

ts_ops = ['ts_rank', 'ts_zscore', 'ts_av_diff', 'ts_mean', 'ts_std_dev']
group_ops = ['group_rank', 'group_zscore', 'group_neutralize', 'group_scale']
days = [5, 10, 20, 60, 200]
groups = ['market', 'industry', 'subindustry', 'sector', 'densify(pv13_h_f1_sector)']

flow_templates = {
    'value': ['PE', 'PB', 'ROE'],
    'quality': ['EPS', 'NetProfitMargin'],
    'reversal': ['returns', 'momentum_factor'],
    'volatility': ['volatility_factor', 'beta'],
    'hybrid': ['EPS', 'fnd6_fic']
}

positive_examples = [
    'group_rank((fnd6_cptnewqv1300_dlttq)/assets, subindustry)',
    'group_rank(ts_rank(fnd6_cicurr, 60), densify(pv13_h_f1_sector))',
    'group_rank(ts_av_diff(sales_growth, 200), sector)',
    'trade_when(ts_corr(close, volume, 20) > 0.3, group_zscore(ts_quantile(winsorize(ts_backfill(adv20, 120), std=4), 240),densify(market)), abs(returns) > 0.1)'
]

# =========================
# 日志
# =========================
logging.basicConfig(filename='ga_alpha.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 工具函数
# =========================
def random_alpha_template():
    flow = random.choice(list(flow_templates.keys()))
    factors = flow_templates[flow]
    if flow != 'hybrid':
        factor = random.choice(factors)
        ts_op = random.choice(ts_ops)
        g_op = random.choice(group_ops)
        d = random.choice(days)
        grp = random.choice(groups)
        return f"{g_op}({ts_op}({factor},{d}), {grp})"
    else:
        f1, f2 = random.sample(factors, 2)
        ts1, ts2 = random.choice(ts_ops), random.choice(ts_ops)
        g_op = random.choice(group_ops)
        d = random.choice(days)
        grp = random.choice(groups)
        return f"{g_op}({ts1}({f1},{d})*{ts2}({f2},{d}), {grp})"

def alpha_similarity(alpha1, alpha2):
    tokens1 = set(alpha1.replace('(', ' ').replace(')', ' ').replace(',', ' ').split())
    tokens2 = set(alpha2.replace('(', ' ').replace(')', ' ').replace(',', ' ').split())
    if not tokens1 or not tokens2:
        return 0
    return len(tokens1 & tokens2) / len(tokens1 | tokens2)

def compute_fitness(alpha):
    sim_score = max([alpha_similarity(alpha, p) for p in positive_examples])
    num_ops = sum([1 for op in ts_ops + group_ops if op in alpha])
    return sim_score * 0.6 + (num_ops / 10) * 0.4

def save_progress(index):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(index))

def load_progress(default_start=0):
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return int(f.read().strip())
        except:
            return default_start
    return default_start

# =========================
# 登录 Brain
# =========================
def sign_in():
    with open(os.path.expanduser('brain_credentials.txt')) as f:
        username, password = json.load(f)
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    resp = sess.post('https://api.worldquantbrain.com/authentication')
    print(resp.status_code, resp.json())
    return sess

# =========================
# 提交 Alpha
# =========================
def submit_alpha(sess, alpha_expr):
    alpha_data = {
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
            "visualization": False
        },
        "regular": alpha_expr
    }
    resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha_data)
    if 'Location' not in resp.headers:
        raise Exception("No Location header")
    return resp.headers['Location']

# =========================
# GA + 自动提交主函数
# =========================
def run_ga_submit():
    sess = sign_in()
    population = [random_alpha_template() for _ in range(POP_SIZE)]
    start_index = load_progress(0)
    logging.info(f"Starting from index {start_index}")

    for gen in range(GENERATIONS):
        logging.info(f"=== Generation {gen} ===")
        fitness_scores = [compute_fitness(alpha) for alpha in population]
        sorted_pop = [x for _, x in sorted(zip(fitness_scores, population), key=lambda pair: pair[0], reverse=True)]
        survivors = sorted_pop[:POP_SIZE // 2]

        # 交叉生成新个体
        offspring = []
        while len(offspring) + len(survivors) < POP_SIZE:
            if random.random() < CROSSOVER_RATE:
                p1, p2 = random.sample(survivors, 2)
                try:
                    split1 = p1.find('(')
                    split2 = p2.find('(')
                    child = p1[:split1+1] + p2[split2+1:]
                    offspring.append(child)
                except:
                    offspring.append(random_alpha_template())
            else:
                offspring.append(random_alpha_template())

        # 变异
        for i in range(len(offspring)):
            if random.random() < MUTATION_RATE:
                offspring[i] = random_alpha_template()

        # 新种群
        population = survivors + offspring

        # 多样性过滤
        new_population = []
        for alpha in population:
            if all(alpha_similarity(alpha, existing) < SIMILARITY_THRESHOLD for existing in new_population):
                new_population.append(alpha)
        while len(new_population) < POP_SIZE:
            new_population.append(random_alpha_template())
        population = new_population

        # 提交 Alpha
        for idx, alpha_expr in enumerate(population):
            global_index = gen * POP_SIZE + idx
            if global_index < start_index:
                continue
            failure_count = 0
            while True:
                try:
                    location = submit_alpha(sess, alpha_expr)
                    logging.info(f"Submitted [{global_index}] {alpha_expr} -> {location}")
                    print(f"[{global_index}] {alpha_expr} -> {location}")
                    save_progress(global_index + 1)
                    break
                except Exception as e:
                    failure_count += 1
                    logging.error(f"Submit failed [{global_index}] {alpha_expr}: {e}")
                    sleep(15)
                    if failure_count >= FAILURE_TOLERANCE:
                        logging.error(f"Exceeded retries, re-login & skip [{global_index}]")
                        sess = sign_in()
                        failure_count = 0
                        save_progress(global_index + 1)
                        break

if __name__ == "__main__":
    run_ga_submit()
