from sklearn.decomposition import PCA
from datetime import datetime
import pandas as pd
import numpy as np
import pytz

def add_bsp(frame):
    frame['bsp'] = (((frame['ask_price'] + frame['bid_price'])/2)-frame['price'])/((frame['ask_price'] - frame['bid_price'])/2)
    frame['bsp'] = frame['bsp'].clip(lower=-1, upper=1)
    return frame

def filter_trading_hours(df, date_str):
    start = datetime.strptime(f"{date_str} 13:30:0", '%Y%m%d %H:%M:%S')
    end = datetime.strptime(f"{date_str} 20:00:00", '%Y%m%d %H:%M:%S')
    df['dt'] = pd.to_datetime(df['participant_timestamp'], unit='ns', origin='unix').copy()
    df = df[(df['dt']>start)&(df['dt']<end)].copy()
    return df


"""
THIS IS WRRROOOONG!!!!!
"""
def add_is_wave(frame, q=0.99, denominator=100_000_000):
    frame['decisecond'] = frame['participant_timestamp'] // denominator
    # TODO: CHECK IF THIS IS CORRECT!!!
    #frame['decisecond_nunique'] = frame.drop_duplicates(subset=['ticker', 'decisecond']).groupby(['decisecond'])['ticker'].transform('nunique')
    frame['decisecond_nunique'] = frame.groupby(['decisecond'])['ticker'].transform('nunique')
    count_cut_off = frame.groupby('decisecond')['ticker'].nunique().quantile(q)
    index =  (frame['decisecond_nunique'] > count_cut_off)
    frame['is_wave'] = np.int8(0)
    frame.loc[index, 'is_wave'] = np.int8(1)
    return frame

def add_is_wave_by_hour(frame, q=0.99, denominator=100_000_000):
    frame['decisecond'] = frame['participant_timestamp'] // denominator
    frame['decisecond_nunique'] = frame.groupby(['decisecond'])['ticker'].transform('nunique')
    frame['hour'] = frame['participant_timestamp'] // (1800*(10**9))
    frame_reduced = frame.drop_duplicates('decisecond')
    frame_reduced['cut_off'] = frame_reduced.groupby('hour')['decisecond_nunique'].transform(lambda s: s.quantile(q))
    frame = frame.merge(frame_reduced[['decisecond', 'cut_off']], on='decisecond')
    #frame['cut_off'] = frame.groupby(['hour'])['decisecond_nunique'].transform(lambda s: s.drop_duplicates) # this is not exactly correct...
    index =  (frame['decisecond_nunique'] > frame['cut_off'])
    frame['is_wave'] = np.int8(0)
    frame.loc[index, 'is_wave'] = np.int8(1)
    return frame

def add_is_wave_likelihood(frame, q=0.99):
    if 'dt' not in frame.columns:
        frame['dt'] = pd.to_datetime(frame['participant_timestamp'], unit='ns', origin='unix').copy()
    frame['min'] = frame['dt'].dt.round('5min')
    frame['decisecond'] = frame['participant_timestamp'] // (10 ** 8)
    frame['min_count'] = frame.groupby(['ticker', 'min'])['decisecond'].transform('nunique')
    frame['likelihood'] = frame['min_count'].clip(lower=1) / 3000
    import numpy as np
    frame['log_likelihood'] = np.log(frame['likelihood'])
    s = frame.drop_duplicates(subset=['ticker', 'decisecond']).groupby('decisecond')['log_likelihood'].sum()
    ll = s.to_frame().rename(columns={'log_likelihood': 'log_likelihood_sum'})
    frame = frame.merge(ll, on='decisecond')
    likelihood_cut_off = s.quantile(1-q)
    frame['is_wave'] = np.int8(0)
    index = (frame['log_likelihood_sum'] < likelihood_cut_off)
    frame.loc[index, 'is_wave'] = np.int8(1)
    return frame

def add_mixed_is_wave(frame, q=0.99):
    if 'dt' not in frame.columns:
        frame['dt'] = pd.to_datetime(frame['participant_timestamp'], unit='ns', origin='unix').copy()
    frame['min'] = frame['dt'].dt.round('5min')
    frame['decisecond'] = frame['participant_timestamp'] // (10 ** 8)
    frame['min_count'] = frame.groupby(['ticker', 'min'])['decisecond'].transform('nunique')
    frame['likelihood'] = frame['min_count'].clip(lower=1) / 3000
    import numpy as np
    frame['log_likelihood'] = np.log(frame['likelihood'])
    s = frame.drop_duplicates(subset=['ticker', 'decisecond']).groupby('decisecond')['log_likelihood'].sum()
    ll = s.to_frame().rename(columns={'log_likelihood': 'log_likelihood_sum'})
    frame = frame.merge(ll, on='decisecond')
    likelihood_cut_off = s.quantile(1-q)
    frame['is_wave'] = np.int8(0)
    likelihood_index = (frame['log_likelihood_sum'] < likelihood_cut_off)

    frame['decisecond_nunique'] = frame.drop_duplicates(subset=['ticker', 'decisecond']).groupby(['decisecond'])['ticker'].transform('nunique')
    count_cut_off = frame.groupby('decisecond')['ticker'].nunique().quantile(q)
    count_index =  (frame['decisecond_nunique'] > count_cut_off)

    frame['is_wave'] = np.int8(0)
    index = likelihood_index | count_index
    frame.loc[index, 'is_wave'] = np.int8(1)
    return frame

def alt_add_is_wave(frame, n=150, denominator=100_000_000):
    frame['decisecond'] = frame['participant_timestamp'] // denominator
    frame['decisecond_nunique'] = frame.groupby(['decisecond'])['ticker'].transform('nunique')

    reduced_frame = frame.drop_duplicates('decisecond')
    deciseconds = reduced_frame.sort_values('decisecond_nunique', ascending=False).head(n)['decisecond'].values
    #print(deciseconds)
    frame['is_wave'] = np.int8(0)
    frame.loc[frame['decisecond'].isin(deciseconds), 'is_wave'] = np.int8(1)
    return frame


def add_new_is_wave(df, period=10_000_000, q=0.99):
    df['period'] = df['participant_timestamp']//period
    df['period_count'] = df.groupby('period')['ticker'].transform('nunique')
    cutoff = df.drop_duplicates('period')['period_count'].quantile(q)

    # we need the opposite sign of is wave to make the cumsum trick work
    df['is_mini_wave'] = 1
    df.loc[df['period_count']>=cutoff, 'is_mini_wave'] = 0

    # note that in non-wave mini_wave_cumsum also will be a constant
    df.loc[:, "mini_wave_cumsum"] = df.drop_duplicates('period', keep='first')['is_mini_wave'].cumsum()

    # mini_wave_cumsum is also a wave id
    df['mini_wave_cumsum'] = df['mini_wave_cumsum'].ffill()
    df['series_length'] = df.groupby('mini_wave_cumsum')['participant_timestamp'].transform('size')

    # change is_mini_wave
    df['is_wave'] = 0
    df.loc[df['is_mini_wave']==0, 'is_wave'] = 1
    df = df.rename(columns={'mini_wave_cumsum':'wave_id'})
    df.loc[df['is_wave']==0, 'wave_id'] = None
    print(len(df['wave_id'].unique()))
    return df



def alt_wave(df, period=10_000_000, q=0.995):
    df['period'] = df['participant_timestamp']//period
    df['period_count'] = df.groupby('period')['ticker'].transform('nunique')
    cutoff = df.drop_duplicates('period')['period_count'].quantile(q)

    df['is_not_mini_wave'] = 1
    df.loc[df['period_count'] >= cutoff, 'is_not_mini_wave'] = 0

    reduced_df = df.drop_duplicates('period', keep='first')
    reduced_df['mini_wave_cumsum'] = reduced_df['is_not_mini_wave'].cumsum()
    # a sequence of mini wave will have the same mini_wave_cumsum
    # reduced_df['period_rank'] = reduced_df.groupby('mini_wave_cumsum')['period'].transform('rank')
    reduced_df.loc[reduced_df['is_not_mini_wave']==0, 'mini_wave_cumsum'] = reduced_df['mini_wave_cumsum'] + 0.5
    reduced_df = reduced_df[['period', 'mini_wave_cumsum']].copy()

    df = df.merge(reduced_df, on='period')

    df['is_wave'] = 0
    df.loc[df['is_not_mini_wave']==0, 'is_wave'] = 1
    df = df.rename(columns={'mini_wave_cumsum': 'wave_id'})


    return df

def only_top_n_stocks(df, n=1000):
    tickers = df.groupby('ticker')['participant_timestamp'].nunique().sort_values(ascending=False)[:n].index
    df = df[df['ticker'].isin(tickers)]
    return df

def no_diag(corr):
    return corr - pd.DataFrame(np.eye(corr.values.shape[0], corr.values.shape[1]), index=corr.index, columns=corr.columns)


def get_wave_corr(df, start_time, end_time, date_str, demean=True, q=0.99, agg_method='mean', variable='dummy', selection_method='count', only_n_stocks=None, return_frame=False, drop_n_stocks=False, denominator=100_000_000, n=None):
    if isinstance(start_time, str):
        start = datetime.strptime(f"{date_str} {start_time}", '%Y%m%d %H:%M:%S')
        end = datetime.strptime(f"{date_str} {end_time}", '%Y%m%d %H:%M:%S')
    else:
        start = start_time
        end = end_time

    df['dt'] = pd.to_datetime(df['participant_timestamp'], unit='ns', origin='unix').copy()
    frame = df[(df['dt'] > start) & (df['dt'] < end)].copy()
    # df[(df['dt']>start)&(df['dt']<=end_time)]

    print(f"Length of frame after time filer: {len(frame)}")

    if only_n_stocks is not None:
        frame = only_top_n_stocks(frame, n=only_n_stocks)

    if drop_n_stocks:
        to_keep = frame.groupby('ticker').size().sort_values(ascending=False).index.values[drop_n_stocks:]
        frame = frame[frame['ticker'].isin(to_keep)]

    if selection_method == 'count':
        frame = add_is_wave(frame, q=q, denominator=denominator)
    elif selection_method == 'likelihood':
        frame = add_is_wave_likelihood(frame, q=q)
    elif selection_method == 'mixed':
        frame = add_mixed_is_wave(frame, q=q)
    elif selection_method == 'mini_wave':
        frame = alt_wave(frame, period=10_000_000, q=q)
    elif selection_method == 'count_by_hour':
        frame = add_is_wave_by_hour(frame, q=q, denominator=denominator)
    elif selection_method == 'alt_wave':
        frame = alt_add_is_wave(frame, denominator=denominator, n=n)
    else:
        raise ValueError('Invalid selection method')

    if 'wave_id' not in frame.columns:
        frame['wave_id'] = frame['decisecond']

    wave_ids = frame[frame['is_wave'] == 1]['wave_id'].unique()
    frame = frame[frame['wave_id'].isin(wave_ids)]
    frame['dummy'] = 1

    #print(frame.head())

    # create multi-index
    tickers = frame['ticker'].unique()
    wave_ids = frame[frame['is_wave'] == 1]['wave_id'].unique()
    index = pd.MultiIndex.from_product([tickers, wave_ids], names = ['tickers', 'wave_id'])

    print(f"Number of tickers: {len(tickers)}")

    if agg_method == 'mean':
        frame = frame.groupby(['ticker', 'wave_id'])[variable].mean().reset_index()
    else:
        frame = frame.drop_duplicates(subset=['ticker', 'wave_id'])

    #print(frame.head())

    if demean:
        m = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack().mean(axis=1)
        frame = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack()
        frame = frame.sub(m, axis=0)
    else:
        frame = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack()
    corr = frame.corr()

    corr = no_diag(corr)
    if return_frame:
        return corr, frame
    else:
        return corr


def filter_out_etfs(df, year, month, data_path=None):
    if data_path is None:
        data_path = '/Volumes/Extreme SSD/data/sec/'
    sec = pd.read_csv(f"{data_path}{year}-{month}.csv")
    tickers = sec[sec['type']=='CS']['ticker'].unique()
    df = df[df['ticker'].isin(tickers)]
    return df


def alt_get_wave_corr(df, start_time, end_time, date_str, demean=True, q=0.99, agg_method='mean', variable='dummy',
                  selection_method='count', only_n_stocks=None, return_frame=False, drop_n_stocks=False,
                  denominator=100_000_000, n=None, wave_filters=None):
    tz = pytz.timezone('America/New_York')

    if isinstance(start_time, str):
        start = datetime.strptime(f"{date_str} {start_time}", '%Y%m%d %H:%M:%S')
        end = datetime.strptime(f"{date_str} {end_time}", '%Y%m%d %H:%M:%S')
        start = tz.localize(start)
        end = tz.localize(end)
    else:
        start = start_time
        end = end_time

    df['dt'] = pd.to_datetime(df['participant_timestamp'], unit='ns', origin='unix', utc=True).copy()
    df['dt'] = df['dt'].dt.tz_convert('America/New_York')

    frame = df[(df['dt'] > start) & (df['dt'] < end)].copy()
    # df[(df['dt']>start)&(df['dt']<=end_time)]

    print(f"Length of frame after time filer: {len(frame)}")

    if only_n_stocks is not None:
        frame = only_top_n_stocks(frame, n=only_n_stocks)

    if drop_n_stocks:
        to_keep = frame.groupby('ticker').size().sort_values(ascending=False).index.values[drop_n_stocks:]
        frame = frame[frame['ticker'].isin(to_keep)]

    if selection_method == 'count':
        frame = add_is_wave(frame, q=q, denominator=denominator)
    elif selection_method == 'likelihood':
        frame = add_is_wave_likelihood(frame, q=q)
    elif selection_method == 'mixed':
        frame = add_mixed_is_wave(frame, q=q)
    elif selection_method == 'mini_wave':
        frame = alt_wave(frame, period=10_000_000, q=q)
    elif selection_method == 'count_by_hour':
        frame = add_is_wave_by_hour(frame, q=q, denominator=denominator)
    elif selection_method == 'alt_wave':
        frame = alt_add_is_wave(frame, denominator=denominator, n=n)
    else:
        raise ValueError('Invalid selection method')

    if 'wave_id' not in frame.columns:
        frame['wave_id'] = frame['decisecond']

    wave_ids = frame[frame['is_wave'] == 1]['wave_id'].unique()
    frame = frame[frame['wave_id'].isin(wave_ids)]
    frame['dummy'] = 1

    if wave_filters is not None:
        for f in wave_filters:
            frame = f(frame)

    # create multi-index
    tickers = frame['ticker'].unique()
    wave_ids = frame[frame['is_wave'] == 1]['wave_id'].unique()
    index = pd.MultiIndex.from_product([tickers, wave_ids], names=['tickers', 'wave_id'])

    print(f"Number of tickers: {len(tickers)}")

    if agg_method == 'mean':
        frame = frame.groupby(['ticker', 'wave_id'])[variable].mean().reset_index()
    else:
        frame = frame.drop_duplicates(subset=['ticker', 'wave_id'])

    # print(frame.head())

    if demean:
        m = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack().mean(axis=1)
        frame = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack()
        frame = frame.sub(m, axis=0)
    else:
        frame = frame.set_index(['ticker', 'wave_id'])[variable].reindex(index).fillna(0).unstack()
    corr = frame.corr()

    corr = no_diag(corr)
    if return_frame:
        return corr, frame
    else:
        return corr



def filter_time(df, start_time, end_time, date_str):
    df['dt'] = pd.to_datetime(df['participant_timestamp'], unit='ns', origin='unix', utc=True).copy()
    df['dt'] = df['dt'].dt.tz_convert('America/New_York')
    start = datetime.strptime(f"{date_str} {start_time}", '%Y%m%d %H:%M:%S')
    end = datetime.strptime(f"{date_str} {end_time}", '%Y%m%d %H:%M:%S')
    tz = pytz.timezone('America/New_York')
    start = tz.localize(start)
    end = tz.localize(end)
    df = df[(df['dt'] >= start) & (df['dt'] <= end)]
    return df


def center_waves(df):
    pass

