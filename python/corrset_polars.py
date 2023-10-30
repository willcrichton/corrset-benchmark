# Copied with modifications from https://github.com/abstractqqq/k_corrset_polars

import polars as pl
import math
import numpy as np
from itertools import combinations, islice
from tqdm.auto import tqdm
from typing import Optional

def k_corrset(df:pl.DataFrame, k: int, u_col:str="user", q_col:str="question", score_col:str="score"):

    # Reduction: Only Keep Users who answer >= k questions
    good_pop = df.group_by(
        pl.col(u_col)
    ).count().filter(
        pl.col("count") >= k
    ).drop_in_place(u_col)

    df_local = df.filter(
        pl.col(u_col).is_in(good_pop)
    ).sort(pl.col(u_col))

    # Precompute grand totals
    grand_totals = df_local.lazy().group_by(
        pl.col(u_col)
    ).agg(
        pl.col(score_col).sum().alias("grand_totals")
    ).cache().collect().lazy()

    batch_size = 1024
    unique_questions = df_local.get_column(q_col).unique()
    all_combs = combinations(unique_questions, k)
    # ((n - k + 1)..=n).product::<usize>() / (1..=k).product::<usize>()
    n = len(unique_questions)
    n_combs = int(np.prod(range(n - k + 1, n+1))) // int(np.prod(range(1, k+1)))
    pbar = tqdm(total=n_combs)
    top_subset: Optional[tuple] = None
    top_corr: float = -1.0
    for i in range(n_combs//batch_size + 1):
        combs = list(islice(all_combs, batch_size))
        frames = (
            df_local.lazy().filter(
                pl.col(q_col).is_in(subset)
            ).group_by(
                pl.col(u_col).set_sorted()
            ).agg(
                pl.count(),
                pl.col(score_col).sum().alias("qs_totals")
            ).filter(
                pl.col("count").eq(k)
            ).select(
                pl.col(u_col).set_sorted(),
                pl.col("qs_totals")
            ).join(
                grand_totals
                , on = u_col
            ).select(
                pl.corr(pl.col("qs_totals"), pl.col("grand_totals")).alias("corr")
            )
            for subset in combs
        )

        for i, frame in enumerate(pl.collect_all(frames)):
            if len(frame) >= 1:
                idx, corr = i, frame.item(0,0)
                if corr is not None:
                    if (not np.isnan(corr)) & (corr > top_corr):
                        top_subset = combs[idx]
                        top_corr = corr
        
        pbar.update(batch_size)

    pbar.close()
    return top_subset, top_corr

data = pl.read_json('../data/data-large.json')
print(k_corrset(data, k=5))