import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor


# Загружаем датасет
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])           # Преобразуем строку в формат даты
    return df


# Скользящее среднее и стандартнео отоклонение
def add_rolling_stats(df: pd.DataFrame, window: int = 30) -> pd.DataFrame:
    df = df.sort_values("timestamp").copy()
    df["rolling_mean"] = (
        df.groupby("city")["temperature"]
        .rolling(window)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["rolling_std"] = (
        df.groupby("city")["temperature"]
        .rolling(window)
        .std()
        .reset_index(level=0, drop=True)
    )
    return df


# Находим аномали
def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["upper"] = df["rolling_mean"] + 2 * df["rolling_std"]            # Верхняя граница нормы
    df["lower"] = df["rolling_mean"] - 2 * df["rolling_std"]            # Нижняя граница
    df["anomaly"] = (
        (df["temperature"] > df["upper"]) |
        (df["temperature"] < df["lower"])
    )
    return df


# Статистика по всем сезонам всех городов
def seasonal_stats(df: pd.DataFrame) -> pd.DataFrame:
    stats = (
        df.groupby(["city", "season"])["temperature"]
        .agg(["mean", "std"])
        .reset_index()
    )
    stats = stats.rename(columns={
        "mean": "season_mean",
        "std": "season_std",
    })
    return stats


# Анализ одного города (скользящее среднее + аномалии) для параллельного запуска
def analyze_city(df: pd.DataFrame, city: str) -> pd.DataFrame:
    city_df = df[df["city"] == city].copy()
    city_df = add_rolling_stats(city_df)
    city_df = detect_anomalies(city_df)
    return city_df


# Анализ всех горолдов без параллельности
def analyze_all_cities(df: pd.DataFrame) -> pd.DataFrame:
    df = add_rolling_stats(df)
    df = detect_anomalies(df)
    return df

# Параллельный анализ и сравнение времени
def analyze_parallel(df: pd.DataFrame) -> dict:
    cities = df["city"].unique()
    start = time.time()
    analyze_all_cities(df)
    time_seq = time.time() - start
    start = time.time()
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda c: analyze_city(df, c), cities))
    result_parallel = pd.concat(results, ignore_index=True)
    time_par = time.time() - start

    return {
        "result": result_parallel,
        "time_seq": time_seq,
        "time_para": time_par,
        "speedup": time_seq / time_par
    }


# Возвращает сезонную статистику для одного города
def get_season_stats(df: pd.DataFrame, city: str) -> pd.DataFrame:
    stats = seasonal_stats(df)
    return stats[stats["city"] == city].reset_index(drop=True)