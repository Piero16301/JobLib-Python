import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm


def calculate_most_used_name(df):
    name_counts = df.groupby('name')['count'].sum()
    return name_counts.idxmax()


if __name__ == '__main__':
    data = pd.read_csv('baby-names-state.csv')
    ca_data = data[data['state_abb'] == 'CA']

    names = Parallel(n_jobs=-1)(delayed(calculate_most_used_name)(df) for _, df in tqdm(ca_data.groupby('year')))

    most_used_name = max(set(names), key=names.count)
    print("The most used name in California is:", most_used_name)
