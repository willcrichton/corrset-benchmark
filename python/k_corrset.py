from itertools import combinations
import pandas as pd
from pandas import IndexSlice as islice
from tqdm.auto import tqdm
import time
import numpy as np

def k_corrset(data, K):
    all_qs = data.question.unique()
    q_to_score = data.set_index(['question', 'user'])
    grand_totals = data.groupby('user').score.sum().rename('grand_total')
    
    corrs = []
    times = []
    for qs in tqdm(combinations(all_qs, K)):
        start = time.time()
        qs_data = q_to_score.loc[islice[qs,:],:].swaplevel()
        answered_all = qs_data.groupby(level=[0]).size() == K
        answered_all = answered_all[answered_all].index
        qs_total = qs_data.loc[islice[answered_all,:]].groupby(level=[0]).sum().rename(columns={'score': 'qs'})
        r = qs_total.join(grand_totals).corr().qs.grand_total
        corrs.append({'qs': qs, 'r': r})
        times.append(time.time() - start)

        if len(times) == 1000:
            print(np.array(times).mean())
            
    corrs = pd.DataFrame(corrs)

    return corrs.sort_values('r', ascending=False).iloc[0].qs

data = pd.read_json('../data/data-large.json')
print(k_corrset(data, K=5))