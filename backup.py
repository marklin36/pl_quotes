import polars as pl
import numpy as np
import pandas as pd

def prepare_frame_general(df:pl.DataFrame, i:int, shift_ns:int, grid_ns:int, n_periods:int)->pl.DataFrame:
    """
    This function creates a new fixed grid by shifting the timestamp and OVERWRITING it.
    """
    d = df.clone()
    d = d.with_columns(
        (pl.col('participant_timestamp') + (i*shift_ns)).alias('participant_timestamp'),
    )
    d = d.with_columns(
        (pl.col('participant_timestamp') // grid_ns).alias('grid')
    )
    d = d.with_columns(
        (pl.col('grid') % n_periods).alias('grid_mod_n')
    )

    d = d.filter(pl.col('bsp').abs()>0.05)
    d = d.with_columns(
        pl.when(pl.col('bsp')>0)
        .then(1)
        .otherwise(-1)
        .alias('bsp')
    )
    return d

def get_z_score_general(d: pl.DataFrame, upper_count:int=5) -> pl.DataFrame:
    """
    This function computes z-score for each ticker and period corresponding to testing a hypothesis that
    bsp is the same in the kernel and outside of the kernel. The output frame contains z-score for every
    ticker and for every period.
    """

    # we don't want a single decisecond to have too large impact
    kernel = d.group_by(['ticker', 'grid_mod_n', 'grid']).agg([
        pl.col("bsp").count().alias("dec_count"),
        pl.col("bsp").mean().alias("dec_mean")
    ])
    kernel = kernel.with_columns(
        pl.col("dec_count").clip(upper_bound=upper_count).alias("dec_count_clipped")
    )
    kernel = kernel.with_columns((pl.col("dec_mean") * pl.col("dec_count_clipped")).alias("bsp_weighted"))

    # now we only group by ticker, and period_col (decisecond_mod_5)
    kernel = kernel.group_by(['ticker', 'grid_mod_n']).agg([
        pl.col("dec_count_clipped").sum().alias("size"),
        pl.col("bsp_weighted").sum().alias("bsp_weighted_sum")
    ])
    kernel = kernel.with_columns((kernel['bsp_weighted_sum']/kernel['size']).alias("mean"))


    # it is important to compute the prior mean in the same way as for the kernel
    prior = d.group_by(["ticker", "grid"]).agg([
        pl.col("bsp").count().alias("prior_dec_count"),
        pl.col("bsp").mean().alias("prio_dec_mean")
    ])
    prior = prior.with_columns(
        pl.col("prior_dec_count").clip(upper_bound=upper_count).alias("prior_dec_count_clipped")
    )
    prior = prior.with_columns(
        (pl.col("prio_dec_mean") * pl.col("prior_dec_count_clipped")).alias("prior_bsp_weighted")
    )
    prior = prior.group_by(['ticker']).agg([
        pl.col("prior_dec_count_clipped").sum().alias("prior_size"),
        pl.col("prior_bsp_weighted").sum().alias("prior_sp_weighted_sum")
    ])
    prior = prior.with_columns((prior['prior_sp_weighted_sum']/prior['prior_size']).alias("prior_bsp_mean"))


    #x = kernel.join(prior, on='ticker', how='left')
    x = pl.concat([kernel, prior], how='align')

    x = x.with_columns(
        ((pl.col('mean')+1)/2.0).alias('mean'),
        ((pl.col('prior_bsp_mean')+1)/2.0).alias('prior_bsp_mean'),
    )

    num = (pl.col("prior_bsp_mean") * (1 - pl.col("prior_bsp_mean")) / pl.col("size")).sqrt()
    z   = (pl.col("mean") - pl.col("prior_bsp_mean")) / num

    x = x.with_columns([
        num.alias("numerator"),
        z.alias("z_score"),
        z.abs().alias("z_score_abs"),
    ])

    return x


def process_date(date, start_time:str, end_time:str, shift_ns:int, grid_ns:int, n_periods:int, upper_count:int, shuffle='decisecond', filters=None):
    d = util.get_time_filtered_trades(
        date_str = date.strftime('%Y%m%d'),
        start_time_str=start_time,
        end_time_str=end_time,
        columns=["ticker", "participant_timestamp", "price", "ask_price", "bid_price", 'exchange']
    )
    d = util.add_bsp(d)

    if filters is not None:
        for filter in filters:
            d = filter(d)
    #d = d.filter(pl.col('exchange')==4)

    n_shifts = grid_ns // shift_ns
    assert grid_ns % shift_ns == 0, f"grid_ns={grid_ns} should be divisible by shift_ns={shift_ns}"

    results = []
    for i in range(0, n_shifts):
        # prepare frame doesn't modify inputs
        df_local = prepare_frame_general(df=d, i=i, shift_ns=shift_ns, grid_ns=grid_ns, n_periods=n_periods)
        r = get_z_score_general(d=df_local, upper_count=upper_count)
        r = r.with_columns(pl.lit(i).alias('shift'))
        r = r.with_columns(
            pl.concat_str([
                pl.col('grid_mod_n'),
                pl.col('shift')
            ], separator='_').alias('kernel')
        )
        results.append(r)
        # statistic = results_to_t(r)
        # results.append(statistic)

    not_random_frame = pl.concat(results)


        # random version
    d_r = d.clone()

    random_results = []
    for i in range(0, 10):
        # prepare frame doesn't modify the input
        df_local = d_r.clone()
        # we do shuffling after defining a new grid but both options are reasonable
        if shuffle=='second':
            df_local = util.add_block_shuffled_time(df_local, block=1000_000_000)
        elif shuffle=='decisecond':
            df_local = util.add_block_shuffled_time(df_local)
        else:
            raise NotImplementedError("the value should be decisecond or second")

        # define new grid
        # preparing the frame MUST BE AFTER SHUFFLING, NOT BEFORE
        df_local = prepare_frame_general(df=df_local, i=i, shift_ns=shift_ns, grid_ns=grid_ns, n_periods=n_periods)

        r = get_z_score_general(d=df_local, upper_count=upper_count)
        r = r.with_columns(pl.lit(i).alias('shift'))
        r = r.with_columns(
            pl.concat_str([
                pl.col('grid_mod_n'),
                pl.col('shift')
            ], separator='_').alias('kernel')
        )

        random_results.append(r)
        # statistic = results_to_t(r)
        # random_results.append(statistic)

    random_frame = pl.concat(random_results)

    return not_random_frame, random_frame
