# Copied from https://github.com/sradc/corrset-benchmark-fork/blob/main/python_optimization/python_optimization.ipynb

import itertools
import time
import numba
import numpy as np
import pandas as pd

def bitset_create(size):
    size_in_int64 = int(np.ceil(size / 64))
    return np.zeros(size_in_int64, dtype=np.int64)

def bitset_add(arr, pos):
    int64_idx = pos // 64
    pos_in_int64 = pos % 64
    arr[int64_idx] |= np.int64(1) << np.int64(pos_in_int64)

def k_corrset(data, K, max_iter=1000):
    data = data.copy()
    data["user"] = data["user"].map({u: i for i, u in enumerate(data.user.unique())})
    data["question"] = data["question"].map(
        {q: i for i, q in enumerate(data.question.unique())}
    )

    all_qs = data.question.unique()
    grand_totals = data.groupby("user").score.sum().values

    # create bitsets
    users_who_answered_q = np.array(
        [bitset_create(data.user.nunique()) for _ in range(data.question.nunique())]
    )
    for q, u in data[["question", "user"]].values:
        bitset_add(users_who_answered_q[q], u)

    score_matrix = np.zeros(
        (data.user.nunique(), data.question.nunique()), dtype=np.int64
    )
    for row in data.itertuples():
        score_matrix[row.user, row.question] = row.score

    qs_combinations = []
    for i, qs in enumerate(itertools.combinations(all_qs, K)):
        if i == max_iter:
            break
        qs_combinations.append(qs)
    qs_combinations = np.array(qs_combinations)

    start = time.time()
    r_vals = compute_corrs(
        qs_combinations, users_who_answered_q, score_matrix, grand_totals
    )
    avg_iter_time_secs = (time.time() - start) / max_iter
    corrs = pd.DataFrame({"qs": [tuple(qs) for qs in qs_combinations], "r": r_vals})
    return corrs, avg_iter_time_secs

@numba.njit(boundscheck=False, fastmath=True, parallel=True, nogil=True)
def compute_corrs(qs_combinations, users_who_answered_q, score_matrix, grand_totals):
    num_qs = qs_combinations.shape[0]
    bitset_size = users_who_answered_q[0].shape[0]
    corrs = np.empty(qs_combinations.shape[0], dtype=np.float64)
    for i in numba.prange(num_qs):
        # bitset will contain users who answered all questions in qs_array[i]
        bitset = users_who_answered_q[qs_combinations[i, 0]].copy()
        for q in qs_combinations[i, 1:]:
            bitset &= users_who_answered_q[q]
        # retrieve stats for the users and compute corrcoef
        n = 0.0
        sum_a = 0.0
        sum_b = 0.0
        sum_ab = 0.0
        sum_a_sq = 0.0
        sum_b_sq = 0.0
        for idx in range(bitset_size):
            if bitset[idx] != 0:
                for pos in range(64):
                    if (bitset[idx] & (np.int64(1) << np.int64(pos))) != 0:
                        score_for_qs = 0.0
                        for q in qs_combinations[i]:
                            score_for_qs += score_matrix[idx * 64 + pos, q]
                        score_for_user = grand_totals[idx * 64 + pos]
                        n += 1.0
                        sum_a += score_for_qs
                        sum_b += score_for_user
                        sum_ab += score_for_qs * score_for_user
                        sum_a_sq += score_for_qs * score_for_qs
                        sum_b_sq += score_for_user * score_for_user
        num = n * sum_ab - sum_a * sum_b
        den = np.sqrt(n * sum_a_sq - sum_a**2) * np.sqrt(n * sum_b_sq - sum_b**2)
        corrs[i] = np.nan if den == 0 else num / den
    return corrs

data = pd.read_json('../data/data-large.json')
k_corrset(data, K=5, max_iter=10)  # JIT compile the function first, (avoid timing the compilation)
result, timing = k_corrset(data, K=5, max_iter=10000000)
print(f'{timing:.9f}')
