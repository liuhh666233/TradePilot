"""技术分析: MACD / 背离 / 成交量异动"""
import numpy as np
import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    """输入含 close 列的 DataFrame，返回附加 DIF/DEA/MACD 列。"""
    df = df.copy()
    df["ema12"] = ema(df["close"], 12)
    df["ema26"] = ema(df["close"], 26)
    df["dif"] = df["ema12"] - df["ema26"]
    df["dea"] = ema(df["dif"], 9)
    df["macd"] = (df["dif"] - df["dea"]) * 2
    return df


def detect_cross(df: pd.DataFrame) -> list[dict]:
    """检测 DIF/DEA 金叉死叉。df 需含 dif, dea, date 列。"""
    signals = []
    prev_diff = None
    for i, row in df.iterrows():
        curr_diff = row["dif"] - row["dea"]
        if prev_diff is not None:
            if prev_diff <= 0 < curr_diff:
                signals.append({"date": str(row["date"]), "type": "golden_cross", "name": "MACD金叉"})
            elif prev_diff >= 0 > curr_diff:
                signals.append({"date": str(row["date"]), "type": "death_cross", "name": "MACD死叉"})
        prev_diff = curr_diff
    return signals


def detect_divergence(df: pd.DataFrame, window: int = 60) -> list[dict]:
    """检测顶背离/底背离。df 需含 close, dif, date 列。"""
    signals = []
    if len(df) < window:
        return signals
    recent = df.tail(window).reset_index(drop=True)
    # 找局部极值 (简化: 用滚动窗口)
    lows = []
    highs = []
    for i in range(2, len(recent) - 2):
        if recent["close"].iloc[i] <= recent["close"].iloc[i - 2:i + 3].min():
            lows.append(i)
        if recent["close"].iloc[i] >= recent["close"].iloc[i - 2:i + 3].max():
            highs.append(i)

    # 底背离: 价格新低但 DIF 未新低
    if len(lows) >= 2:
        i1, i2 = lows[-2], lows[-1]
        if recent["close"].iloc[i2] < recent["close"].iloc[i1] and recent["dif"].iloc[i2] > recent["dif"].iloc[i1]:
            signals.append({"date": str(recent["date"].iloc[i2]), "type": "bull_divergence", "name": "底背离"})

    # 顶背离: 价格新高但 DIF 未新高
    if len(highs) >= 2:
        i1, i2 = highs[-2], highs[-1]
        if recent["close"].iloc[i2] > recent["close"].iloc[i1] and recent["dif"].iloc[i2] < recent["dif"].iloc[i1]:
            signals.append({"date": str(recent["date"].iloc[i2]), "type": "bear_divergence", "name": "顶背离"})

    return signals


def detect_volume_anomaly(df: pd.DataFrame) -> list[dict]:
    """检测成交量异动。df 需含 close, volume, date 列。"""
    signals = []
    if len(df) < 20:
        return signals
    df = df.copy()
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    df["vol_ratio"] = df["volume"] / df["vol_ma5"]
    df["high_20"] = df["close"].rolling(20).max()
    df["low_20"] = df["close"].rolling(20).min()
    df["position"] = (df["close"] - df["low_20"]) / (df["high_20"] - df["low_20"] + 1e-10)

    last = df.iloc[-1]
    # 放量突破
    if last["vol_ratio"] > 2 and last["close"] >= last["high_20"] * 0.98:
        signals.append({"date": str(last["date"]), "type": "volume_breakout", "name": "放量突破"})
    # 高位缩量
    if last["vol_ratio"] < 0.5 and last["position"] > 0.8:
        signals.append({"date": str(last["date"]), "type": "high_shrink", "name": "高位缩量"})
    # 地量
    vol_min_60 = df["volume"].tail(60).min() if len(df) >= 60 else df["volume"].min()
    if last["volume"] <= vol_min_60 * 1.05:
        signals.append({"date": str(last["date"]), "type": "extreme_low_volume", "name": "地量"})

    return signals


def analyze_stock(df: pd.DataFrame) -> dict:
    """对一只股票做完整技术分析。df 需含 date, open, high, low, close, volume。"""
    df = compute_macd(df)
    return {
        "macd": df[["date", "close", "volume", "dif", "dea", "macd"]].to_dict(orient="records"),
        "cross_signals": detect_cross(df),
        "divergence_signals": detect_divergence(df),
        "volume_signals": detect_volume_anomaly(df),
    }
