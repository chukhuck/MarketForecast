import pandas as pd
from datetime import timedelta
from datetime import date


def get_mean_values(
        data,
        years,
        excluded_years=[],
        agg_funcs=['mean', ],
        days_in_cycle=365,
        moving_average_window=10,
        moving_average_min_period=None,
        ema_factor=0.5,
        first_day_type='FM',
        first_days_in_cycles=None):

    # Calculate day of cycle
    dataset = data.assign(day_of_cycle=calc_day_of_cycle(
        data, first_day_type, first_days_in_cycles, days_in_cycle))

    # Filter data
    filtered_dataset = dataset.loc[[
        (index_year in years and index_year not in excluded_years) for index_year in dataset.index.year]]
    filtered_dataset = filtered_dataset[(filtered_dataset['day_of_cycle'] > 0) & (
        filtered_dataset['day_of_cycle'] < days_in_cycle + 1)]

    # Operate
    dataset_by_days_of_cycle = filtered_dataset.groupby(
        'day_of_cycle').agg(agg_funcs)

    # Calculate the first day in a cycle and shift DataFrame to it.
    first_day = get_first_day_in_last_cycle(
        years, first_day_type, first_days_in_cycles)

    dataset_by_days_of_cycle.index = pd.DatetimeIndex(
        [(first_day + timedelta(d - 1)) for d in dataset_by_days_of_cycle.index])

    # Smoothing
    sma_mean = get_normalized_sma(
        data=dataset_by_days_of_cycle,
        window=moving_average_window,
        min_period=moving_average_min_period,
        sma_type='mean')

    sma_median = get_normalized_sma(
        data=dataset_by_days_of_cycle,
        window=moving_average_window,
        min_period=moving_average_min_period,
        sma_type='median')

    ema = get_normalized_ema(dataset_by_days_of_cycle, ema_factor)

    return (sma_mean, sma_median, ema)


def calc_day_of_cycle(data, first_day_type='FM', first_days_in_cycles=[], days_in_cycle=365):
    if first_day_type is 'NY':
        day_of_cycle = [d.timetuple().tm_yday for d in data.index]
    elif first_day_type is 'FM':
        day_of_cycle = [(d.timetuple().tm_wday) + 7 * (d.week - 1)
                        for d in data.index]
    elif first_day_type is 'C':
        if first_days_in_cycles is None:
            raise ValueError(
                'If you choise (first_day=C) the custom first days in cycles' +
                'you have to pass non-empty array of a first days in a cycles (first_days_in_cycles)')
        else:
            temp_series = [[(fd + timedelta(days=c), c + 1)
                            for c in range(days_in_cycle)] for fd in first_days_in_cycles]

            day_of_cycle = pd.Series([d[1] for d in temp_series[0]])
            day_of_cycle.index = [d[0] for d in temp_series[0]]
    else:
        raise ValueError('An unknown type of the first day in cycle.')

    return day_of_cycle


def get_first_day_in_last_cycle(years, first_day_type, first_days_in_cycles):
    if first_day_type is 'NY':
        first_day = date(sorted(years)[-1], 1, 1)
    elif first_day_type is 'FM':
        new_year = date(sorted(years)[-1], 1, 1)
        new_year_wday = new_year.timetuple().tm_wday
        first_day = new_year if new_year_wday is 0 else new_year + \
            timedelta(7 - new_year_wday)
    elif first_day_type is 'C':
        if first_days_in_cycles is None:
            raise ValueError(
                'If you choise (first_day=C) the custom first days in cycles' +
                'you have to pass non-empty array of a first days in a cycles (first_days_in_cycles)')
        else:
            first_day = first_days_in_cycles[-1]
    else:
        raise ValueError('An unknown type of the first day in cycle.')

    return first_day


def get_normalized_sma(data, window, min_period, sma_type):
    sma = data.rolling(window, min_periods=(
        window if min_period is None else min_period))

    aggregated_sma = sma.mean() if sma_type is 'mean' else sma.median()

    return aggregated_sma / aggregated_sma.max()


def get_normalized_ema(data, alpha=0.5, adjust=False):
    ema = data.ewm(alpha=alpha, adjust=adjust).mean()
    return ema / ema.max()
