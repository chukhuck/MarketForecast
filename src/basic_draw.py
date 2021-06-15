import plotly.express as px
import pandas as pd


def draw_basic_plot(filename, title):
    df = pd.read_json('..\\data\\{}.json'.format(filename))

    fig = px.line(df, x='formatted_date', y='adjclose', title=title, )

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ])
        )
    )
    fig.show()
