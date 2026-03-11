import pandas as pd
import plotly.graph_objects as go

file = "debug_ib/ZB.csv"   # cambia qui il file da vedere

df = pd.read_csv(file)
df["date"] = pd.to_datetime(df["date"])

df["MA50"] = df["close"].rolling(50).mean()
df["MA200"] = df["close"].rolling(200).mean()

fig = go.Figure()

fig.add_trace(go.Candlestick(
    x=df["date"],
    open=df["open"],
    high=df["high"],
    low=df["low"],
    close=df["close"],
    name="Price"
))

fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["MA50"],
    mode="lines",
    name="MA50"
))

fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["MA200"],
    mode="lines",
    name="MA200"
))

fig.update_layout(
    title=f"IB Chart - {file}",
    xaxis_title="Date",
    yaxis_title="Price",
    template="plotly_dark",
    xaxis_rangeslider_visible=False
)

output_file = "ib_chart.html"
fig.write_html(output_file, auto_open=True)
print(f"Chart saved to {output_file}")