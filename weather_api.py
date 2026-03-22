import requests
import aiohttp
import asyncio


BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Синхронный запрос к api
def get_temp_sync(city: str, api_key: str) -> dict:
    params = {
        "q": city,                      # Название города
        "appid": api_key,               # Наш API ключ
        "units": "metric"               # Температура в Цельсиях (не Кельвинах)
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if response.status_code != 200:
        return {"error": data.get("message")}
    return {
        "temperature": data["main"]["temp"],                # Текущая температура
        "feels_like": data["main"]["feels_like"],           # Ощущаемая
        "description": data["weather"][0]["description"],   # Описание погоды
        "city": data["name"]                                # Название города от API
    }


# Асинхронный запрос
async def get_temp_async(city: str, api_key: str) -> dict:
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    # aiohttp.ClientSession() -  сессия для HTTP запросов
    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL, params=params) as response:
            data = await response.json()
            return {
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "description": data["weather"][0]["description"],
                "city": data["name"]
            }

# Обёртка для вызова асинхронной функции
def get_temp_async_wrapper(city: str, api_key: str) -> dict:
    return asyncio.run(get_temp_async(city, api_key))


# проверка аномальности текущей температуры
def check_temp_anomaly(curr_temp: float,city: str,season: str,season_stats) -> dict:
    season_row = season_stats[season_stats["season"] == season]
    if season_row.empty:
        return {"error": f"Нет данных для сезона {season}"}
    mean = season_row["season_mean"].values[0]
    std = season_row["season_std"].values[0]
    # Границы нормы
    upper = mean + 2 * std
    lower = mean - 2 * std
    if_anomaly = curr_temp > upper or curr_temp < lower
    return {
        "curr_temp": curr_temp,
        "season_mean": round(mean, 2),
        "season_std": round(std, 2),
        "upper": round(upper, 2),
        "lower": round(lower, 2),
        "if_anomaly": if_anomaly,
        "season": season
    }