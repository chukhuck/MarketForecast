import matplotlib.dates as mdates
from scipy.interpolate import interp1d
import matplotlib.pyplot as plt
import statsmodels.api as sm
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from dateutil.parser import parse
from pandas import DataFrame
from datetime import timedelta


def filter_by_year(data, year, shift=True, norm=True):
    ds = data.loc[[(index_year in [year, ])
                   for index_year in data.index.year]]

    ds = ds / ds.max() if norm else ds
    ds = ds.shift(periods=-365, freq="D") if shift else ds

    return ds
