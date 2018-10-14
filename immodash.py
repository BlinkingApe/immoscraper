import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go

app = dash.Dash()
df = pd.read_csv('current.csv', sep=';')

df['house_size'] = pd.to_numeric(df['house_size'], errors='coerce')

df = df.loc[df['price'] > 80000]

df = df.loc[df['price'] < 250000]

df = df.loc[df['house_size'] < 2000]
app.layout = html.Div([
    dcc.Graph(
        id='life-exp-vs-gdp',
        figure={
            'data': [
                go.Scatter(
                    x=df[df['city'] == i]['price'],
                    y=df[df['city'] == i]['house_size'],
                    text=df[df['city'] == i]['travel'],
                    mode='markers',
                    opacity=0.8,
                    marker={
                        'size': 15,
                        'line': {'width': 0.5, 'color': 'white'}
                    },
                    name=i
                ) for i in df.city.unique()
            ],
            'layout': go.Layout(
                xaxis={'type': 'log', 'title': 'Price in Euro [€]'},
                yaxis={'title': 'House size in m²'},
                margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    )
])

if __name__ == '__main__':
    app.run_server()
