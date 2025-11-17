# decision_tree_guided_alpha_generator_3.py
import os
import json
import logging
import time
import random
import requests
import numpy as np
import pandas as pd
from requests.auth import HTTPBasicAuth
from os.path import expanduser
from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import OneHotEncoder

# -------------------------
# Config / hyperparams
# -------------------------
TARGET_COUNT = 5000                 # 目标 alpha 数量
SEED_SAMPLE_SIZE = 200              # 初始随机种子样本数量（用于训练决策树）
MAX_ATTEMPTS_WITHOUT_NEW = 2000     # 如果连续这么多 candidate 没加入则提前退出
CORR_THRESHOLD = 0.7                # 相关性阈值
TS_LENGTH = 252                     # 用于相关性计算的历史天数（示例：252 天）
POLL_INTERVAL = 4                   # 秒，轮询 simulation 结果的间隔（注意 API 限速）
MAX_WORKERS = 6                     # 并发量（提交+轮询）
PROGRESS_FILE = "progress3.txt"
GENERATED_FILE = "generated_alphas_3.txt"
LOG_FILE = "simulation_3.log"

# 预定义流派与算子（可根据需要扩展）
flow_factors = {
    'value': ['PE', 'PB', 'ROE', 'BookValuePerShare'],
    'quality': ['EPS', 'NetProfitMargin', 'GrossMargin', 'ROA'],
    'reversal': ['returns', 'Close', 'EPS'],
    'volatility': ['Close', 'beta', 'volume'],
    'hybrid': ['EPS', 'fnd6_fic', 'ROE']   # hybrid 用若干因子两两组合
}

ts_ops = ['ts_rank', 'ts_zscore', 'ts_av_diff', 'ts_mean', 'ts_std_dev']
group_ops = ['group_rank', 'group_zscore', 'group_neutralize', 'group_scale']
days_list = [5, 10, 20, 60, 200]
groups = ['market', 'industry', 'subindustry', 'sector']

# -------------------------
# logging
# -------------------------
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# -------------------------
# Authentication
# -------------------------
def sign_in():
    with open(expanduser('brain_credentials.txt')) as f:
        username, password = json.load(f)
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    resp = sess.post('https://api.worldquantbrain.com/authentication')
    if resp.status_code != 200 and resp.status_code!=201:
        raise Exception(f"Login failed: {resp.status_code} {resp.text}")
    logger.info("Signed in successfully")
    return sess

sess = sign_in()

# -------------------------
# Persistence utilities
# -------------------------
def save_progress_index(idx):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(idx))

def load_progress_index(default=0):
    if os.path.exists(PROGRESS_FILE):
        try:
            return int(open(PROGRESS_FILE).read().strip())
        except:
            return default
    return default

def save_generated_alpha(alpha_expr):
    with open(GENERATED_FILE, "a", encoding="utf-8") as f:
        f.write(alpha_expr + "\n")

# -------------------------
# Helper: build deterministic alpha list (flow-full coverage)
# -------------------------
def build_deterministic_alpha_expressions():
    exprs = []
    # single-flow
    for flow, factors in flow_factors.items():
        if flow != 'hybrid':
            for factor in factors:
                for ts in ts_ops:
                    for g in group_ops:
                        for d in days_list:
                            for grp in groups:
                                exprs.append((flow, factor, ts, g, d, grp,
                                             f"{g}({ts}({factor},{d}),{grp})"))
        else:
            # hybrid: pairwise combinations of listed factors, use multiplication combination
            f_list = factors
            for i in range(len(f_list)):
                for j in range(i+1, len(f_list)):
                    f1, f2 = f_list[i], f_list[j]
                    for ts1 in ts_ops:
                        for ts2 in ts_ops:
                            for g in group_ops:
                                for d in days_list:
                                    for grp in groups:
                                        exprs.append((flow, f"{f1}|{f2}", f"{ts1}|{ts2}",
                                                      g, d, grp,
                                                      f"{g}({ts1}({f1},{d})*{ts2}({f2},{d}),{grp})"))
    return exprs

alpha_meta_list = build_deterministic_alpha_expressions()
logger.info(f"Built deterministic alpha space size: {len(alpha_meta_list)}")

# -------------------------
# API helpers: submit simulation & poll result
# -------------------------
def submit_simulation(alpha_expr):
    """
    Submit alpha expression to '/simulations'.
    Returns the Location URL to poll.
    """
    payload = {
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
    }
    resp = sess.post('https://api.worldquantbrain.com/simulations', json=payload)
    if resp.status_code not in (200,201):
        raise Exception(f"Submit failed: {resp.status_code} {resp.text}")
    if 'Location' not in resp.headers:
        raise Exception("No Location header returned")
    return resp.headers['Location']

def poll_simulation_result(location_url, timeout=300):
    """
    Poll the simulation Location until finished, then parse result and return:
      - series: a numpy array of historical daily alpha values (length TS_LENGTH or similar)
      - score: a numeric score for this alpha (e.g. IC or IS_summary['ic'] etc.)
    IMPORTANT: THIS FUNCTION MUST BE ADAPTED to the actual response structure your account gets.
    Here I give a robust template and sample parsing logic.
    """
    start = time.time()
    while True:
        resp = sess.get(location_url)
        if resp.status_code != 200:
            # may be that the location returns 202 while building; just retry
            time.sleep(POLL_INTERVAL)
            if time.time() - start > timeout:
                raise TimeoutError("Polling simulation timed out")
            continue
        data = resp.json()
        # --- Example parsing logic (please adapt) ---
        # Case A: API returns simulation status + results in 'status'/'result' keys
        status = data.get('status', '').lower()
        if status in ('running', 'queued', 'inprogress'):
            time.sleep(POLL_INTERVAL)
            if time.time() - start > timeout:
                raise TimeoutError("Polling simulation timed out")
            continue
        # If finished, try to extract series and score
        # The actual keys vary - below are possible placeholders:
        # - daily series could be in data['results']['alpha_series'] or data['simulationResult']['series']
        # - scoring could be in data['isSummary']['IC'] or data['metrics']['ic']
        series = None
        score = None
        # try a few common keys (you must adapt to your account's response shape)
        if 'result' in data:
            res = data['result']
            # locate a daily series - adapt names as needed
            if 'alpha_series' in res:
                series = np.array(res['alpha_series'])
            elif 'series' in res:
                series = np.array(res['series'])
            # score / ic
            if 'ic' in res:
                score = float(res['ic'])
        if 'isSummary' in data:
            isS = data['isSummary']
            # try to get ic and/or long/short counts
            if 'ic' in isS:
                score = float(isS['ic'])
            # try to extract a "daily" vector if present - unlikely, keep robust
            if 'daily' in isS:
                series = np.array(isS['daily'])
        # fallback: try flattening any list fields
        if series is None:
            # attempt to find any key with list length >= TS_LENGTH
            for k,v in data.items():
                if isinstance(v, list) and len(v) >= 10:
                    series = np.array(v)
                    break
        # If still no series, produce an approximate proxy (e.g., use random but warn)
        if series is None:
            series = np.random.randn(TS_LENGTH)
            logger.warning("Could not parse real series from simulation response; using random proxy. Adapt poll_simulation_result().")
        if score is None:
            # If no IC, compute proxy (e.g., mean(series)) or set to 0
            score = float(np.nanmean(series))
        return series[:TS_LENGTH], score

# -------------------------
# Feature encoding for decision tree
# -------------------------
enc = OneHotEncoder(handle_unknown='ignore', sparse=False)
# We'll encode ts_op, group_op, days, group_field, flow as categorical features
# To fit encoder we need a small example domain; build domain arrays from our lists:
categorical_domains = []
# ts_ops, group_ops, days_list, groups, flows
flows = list(flow_factors.keys())
categorical_domains = [ts_ops, group_ops, list(map(str, days_list)), groups, flows]
# create sample dataframe to fit encoder
sample_rows = []
for ts in ts_ops:
    for g in group_ops:
        for d in days_list:
            for grp in groups:
                for flow in flows:
                    sample_rows.append([ts, g, str(d), grp, flow])
df_sample = pd.DataFrame(sample_rows, columns=['ts','g','d','grp','flow'])
enc.fit(df_sample)

def meta_to_featurevec(meta_tuple):
    # meta_tuple: (flow, factor or factors, ts or ts|ts, g, d, grp, expr)
    flow, factor_repr, ts_repr, g_op, d, grp, expr = meta_tuple
    # extract ts_op(s) and group_op, day, group, flow
    # for hybrid ts_repr might be "ts1|ts2", for single it's "ts"
    ts_field = ts_repr.split("|")[0] if "|" in str(ts_repr) else ts_repr
    row = [[ts_field, g_op, str(d), grp, flow]]
    oh = enc.transform(row)[0]
    return oh

# -------------------------
# Build seed dataset by sampling random alpha_meta and polling results
# -------------------------
def build_seed_dataset(seed_size=SEED_SAMPLE_SIZE):
    X, y = [], []
    sampled = set()
    logger.info("Building seed dataset by random sampling and simulation...")
    attempts = 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        while len(futures) < seed_size and attempts < seed_size*5:
            attempts += 1
            idx = random.randrange(0, len(alpha_meta_list))
            if idx in sampled:
                continue
            sampled.add(idx)
            meta = alpha_meta_list[idx]
            expr = meta[6]
            try:
                loc = submit_simulation(expr)
                futures[ex.submit(poll_simulation_result, loc)] = meta
            except Exception as e:
                logger.error(f"submit failed for seed expr: {e}")
        # collect
        for fut in as_completed(futures):
            meta = futures[fut]
            try:
                series, score = fut.result()
                feat = meta_to_featurevec(meta)
                X.append(feat)
                y.append(score)
            except Exception as e:
                logger.error(f"seed poll failed: {e}")
    logger.info(f"Built seed dataset: {len(X)} samples")
    return np.array(X), np.array(y)

# -------------------------
# Train decision tree model
# -------------------------
def train_tree(X, y):
    if len(X) < 10:
        # Not enough data; return None
        return None
    tree = DecisionTreeRegressor(max_depth=6)
    tree.fit(X, y)
    logger.info("Trained DecisionTreeRegressor.")
    return tree

# -------------------------
# Main loop: guided search + relatedness control + submission
# -------------------------
def main_loop():
    # load progress index
    progress_idx = load_progress_index(0)
    # load already generated alphas if any
    generated_count = 0
    if os.path.exists(GENERATED_FILE):
        with open(GENERATED_FILE, "r", encoding="utf-8") as f:
            generated_count = sum(1 for _ in f)
    logger.info(f"Starting main_loop: already generated {generated_count} alphas, progress idx {progress_idx}")

    # 1) build seed dataset and train initial model
    X_seed, y_seed = build_seed_dataset(SEED_SAMPLE_SIZE)
    tree = train_tree(X_seed, y_seed)
    # if tree is None, fallback to uniform scoring

    simulated_vectors = []   # store historical series for chosen alphas (for corr calculation)
    chosen_alphas = []       # track chosen expressions

    attempts_without_new = 0
    idx = progress_idx

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        # We will iterate through deterministic alpha_meta_list in order,
        # but use model score to prioritize (we can later implement scoring+priority queue)
        while len(chosen_alphas) < TARGET_COUNT and idx < len(alpha_meta_list):
            meta = alpha_meta_list[idx]
            expr = meta[6]
            # prepare feature for prediction
            feat = meta_to_featurevec(meta)
            score = tree.predict([feat])[0] if tree is not None else 0.0

            # Decide to try: threshold or top-k policy.
            # Here: try if predicted score above median of seed scores OR random small prob for exploration
            try_threshold = False
            if tree is not None:
                if score >= np.median(y_seed):
                    try_threshold = True
                else:
                    # small exploration chance
                    if random.random() < 0.02:
                        try_threshold = True
            else:
                try_threshold = True

            if not try_threshold:
                idx += 1
                continue

            # Submit and poll (synchronously here for clarity, can be done via executor)
            try:
                loc = submit_simulation(expr)
                series, real_score = poll_simulation_result(loc)
            except Exception as e:
                logger.error(f"submit/poll failed for idx {idx}: {e}")
                # if failed frequently we might want to re-login
                attempts_without_new += 1
                if attempts_without_new > MAX_ATTEMPTS_WITHOUT_NEW // 10:
                    logger.warning("many failures, re-login")
                    try:
                        global sess
                        sess = sign_in()
                    except:
                        pass
                idx += 1
                continue

            # calculate max correlation with chosen set
            if len(simulated_vectors) == 0:
                max_corr = 0.0
            else:
                corrs = [np.corrcoef(series[:TS_LENGTH], v[:TS_LENGTH])[0,1] for v in simulated_vectors]
                max_corr = np.nanmax(corrs)

            if np.isnan(max_corr):
                max_corr = 0.0

            if max_corr < CORR_THRESHOLD:
                # Accept alpha
                simulated_vectors.append(series[:TS_LENGTH])
                chosen_alphas.append(expr)
                save_generated_alpha(expr)
                save_progress_index(idx + 1)
                logger.info(f"Accepted alpha #{len(chosen_alphas)} idx {idx} corr={max_corr:.3f} score={real_score:.4f}: {expr}")
                print(f"Accepted alpha #{len(chosen_alphas)} corr={max_corr:.3f} score={real_score:.4f}")
                attempts_without_new = 0
                # optionally: update training set with the new (feat, real_score)
                # append to X_seed,y_seed and periodically retrain tree
                X_seed = np.vstack([X_seed, feat])
                y_seed = np.append(y_seed, real_score)
                if len(y_seed) % 50 == 0:
                    tree = train_tree(X_seed, y_seed)
            else:
                # reject due to high correlation
                logger.info(f"Rejected idx {idx} due to high corr {max_corr:.3f}")
                attempts_without_new += 1

            # safety break if too many unsuccessful attempts
            if attempts_without_new >= MAX_ATTEMPTS_WITHOUT_NEW:
                logger.warning("Too many attempts without new accepted alpha -> stopping early.")
                break

            idx += 1

    logger.info(f"Finished. Chosen {len(chosen_alphas)} alphas.")
    return chosen_alphas

if __name__ == "__main__":
    chosen = main_loop()
    print(f"Done. total chosen: {len(chosen)}")
