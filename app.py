import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go  # Для более крутых и продвинутых графиков, где можно работать со слоями
from datetime import datetime

# Импортируем наши файлы
from analysis import (
    load_data,
    analyze_parallel,
    get_season_stats
)
from weather_api import (
    get_temp_sync,
    check_temp_anomaly
)

season_names = {
    "winter": "Зима",
    "spring": "Весна",
    "summer": "Лето",
    "autumn": "Осень"
}

st.set_page_config(
    page_title="Анализ температур",
    layout="wide"
)
st.title("Анализ температурных данных")
st.sidebar.header("Настройки")

uploaded_file = st.sidebar.file_uploader(
    "Загрузите файл с данными (CSV)",
    type=["csv"]
)

api_key = st.sidebar.text_input(
    "API ключ от OpenWeatherMap",
    type="password",  # скрываем введённый текст
    placeholder="Введите ваш API ключ..."
)

if uploaded_file is not None:
    df = load_data(uploaded_file)
    df["season"] = df["season"].map(season_names)
    cities = sorted(df["city"].unique())  # Выпадащий список городов с сортировкой по алфавиту
    selected_city = st.sidebar.selectbox(
        "Выберите город",
        cities
    )
    analyze = analyze_parallel(df)  # Параллельный анализ + время
    analyzed_df = analyze["result"]
    city_df = analyzed_df[analyzed_df["city"] == selected_city].copy()
    city_season_stats = get_season_stats(df, selected_city)

    # Вкладки (приложеения на разделы делаем)
    tab1, tab2, tab3, tab4 = st.tabs([
        "Статистика",
        "Временной ряд",
        "Сезонный профиль",
        "Текущая погода"
    ])

    # Общая статистика
    with tab1:
        st.subheader(f"Общая статистика {selected_city}")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                label="Средняя температура",
                value=f"{city_df["temperature"].mean():.1f} °C"
            )
        with col2:
            st.metric(
                label="Максимальная температура",
                value=f"{city_df["temperature"].max():.1f} °C"
            )
        with col3:
            st.metric(
                label="Минимальная температура",
                value=f"{city_df["temperature"].min():.1f} °C"
            )
        with col4:
            anomaly_cnt = city_df["anomaly"].sum()
            st.metric(
                label="Аномалий найдено",
                value=int(anomaly_cnt)
            )

        st.divider()

        # Таблица сезонной статистики
        st.subheader("Статистика по сезонам")
        dp_stats = city_season_stats.copy()
        dp_stats["season_mean"] = dp_stats["season_mean"].round(2)
        dp_stats["season_std"] = dp_stats["season_std"].round(2)
        dp_stats.columns = ["Город", "Сезон", "Средняя темп. (°C)", "Отклонение (°C)"]
        st.dataframe(dp_stats, use_container_width=True)

        st.divider()

        # График (box plot) распределения температур по сезонам
        st.subheader("Распределение температур по сезонам")
        fig_box = px.box(
            city_df,
            x="season",
            y="temperature",
            color="season",
            title=f"Распределение температур города {selected_city}",
            labels={
                "season": "Сезон",
                "temperature": "Температура (°C)"
            }
        )
        st.plotly_chart(fig_box, use_container_width=True)

        st.divider()

        # Сравнения производительности
        st.subheader("Сравнение скорости анализа")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                label="Последовательно",
                value=f"{analyze["time_seq"]:.2f} сек."
            )
        with col2:
            st.metric(
                label="Параллельно",
                value=f"{analyze["time_para"]:.2f} сек."
            )
        with col3:
            st.metric(
                label="Ускорение",
                value=f"{analyze["speedup"]:.2f}x"
            )
        # Вывод
        st.info(
            """
            **Вывод по распараллеливанию**

            Используем ThreadPoolExecutor для потоков, но из-за GIL ускорение минимальное. Для CPU задач 
            эффективнее ProcessPoolExecutor. Ноо при наших данных разница почти незаметна, а для больших датасетов 
            ProcessPoolExecutor уже даст прирост.
            """
        )

    # Временной ряд с аномалиями
    with tab2:
        st.subheader(f"Временной ряд температур {selected_city}")
        fig_ts = go.Figure()

        # Линия реальной температуры
        fig_ts.add_trace(go.Scatter(
            x=city_df["timestamp"],
            y=city_df["temperature"],
            mode="lines",
            name="Температура",
            line=dict(color="cyan", width=1),
            opacity=0.4
        ))

        # Линия скользящего среднего
        fig_ts.add_trace(go.Scatter(
            x=city_df["timestamp"],
            y=city_df["rolling_mean"],
            mode="lines",
            name="Скользящее среднее (30 дней)",
            line=dict(color="navy", width=2)
        ))

        # Верхняя граница нормы
        fig_ts.add_trace(go.Scatter(
            x=city_df["timestamp"],
            y=city_df["upper"],
            mode="lines",
            name="Верхняя граница",
            line=dict(color="white", dash="solid", width=0.7)
        ))

        # Нижняя граница нормы
        fig_ts.add_trace(go.Scatter(
            x=city_df["timestamp"],
            y=city_df["lower"],
            mode="lines",
            name="Нижняя граница",
            line=dict(color="white", dash="solid", width=0.7)
        ))

        # Аномалии
        anomalies = city_df[city_df["anomaly"] == True]
        fig_ts.add_trace(go.Scatter(
            x=anomalies["timestamp"],
            y=anomalies["temperature"],
            mode="markers",
            name="Аномалии",
            marker=dict(color="red", size=4)
        ))
        fig_ts.update_layout(
            title=f"Температура и аномалии города {selected_city}",
            xaxis_title="Дата",
            yaxis_title="Температура (°C)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        st.plotly_chart(fig_ts, use_container_width=True)

        st.divider()

        st.subheader("Список аномалий")
        if anomalies.empty:
            st.success("Аномалий не найдено!")
        else:
            anomalies_display = anomalies[[
                "timestamp", "temperature", "season", "rolling_mean", "upper", "lower"
            ]].copy()
            anomalies_display.columns = [
                "Дата", "Температура (°C)", "Сезон", "Скользящее среднее", "Верхняя граница", "Нижняя граница"
            ]
            for col in ["Температура (°C)", "Скользящее среднее", "Верхняя граница", "Нижняя граница"]:
                anomalies_display[col] = anomalies_display[col].round(2)
            st.dataframe(anomalies_display, use_container_width=True)

    with tab3:
        st.subheader(f"Сезонный профиль {selected_city}")
        season_order = ["Зима", "Весна", "Лето", "Осень"]
        city_season_stats_sorted = city_season_stats.copy()
        city_season_stats_sorted["season"] = pd.Categorical(
            city_season_stats_sorted["season"],
            categories=season_order,
            ordered=True
        )
        city_season_stats_sorted = city_season_stats_sorted.sort_values("season")

        fig_season = go.Figure()

        # Верхняя граница нормы
        fig_season.add_trace(go.Scatter(
            x=city_season_stats_sorted["season"],
            y=city_season_stats_sorted["season_mean"] + 2 * city_season_stats_sorted["season_std"],
            mode="lines",
            line=dict(color="blue", width=0),
            showlegend=False
        ))

        # Нижняя граница
        fig_season.add_trace(go.Scatter(
            x=city_season_stats_sorted["season"],
            y=city_season_stats_sorted["season_mean"] - 2 * city_season_stats_sorted["season_std"],
            mode="lines",
            line=dict(color="blue", width=0),
            fill="tonexty",
            name="Диапазон нормы",
            opacity=0.8
        ))

        # Линия среднего по сезону
        fig_season.add_trace(go.Scatter(
            x=city_season_stats_sorted["season"],
            y=city_season_stats_sorted["season_mean"],
            mode="lines+markers",
            name="Средняя температура",
            line=dict(color="white", width=3),
            marker=dict(size=10)
        ))

        fig_season.update_layout(
            title=f"Средняя температура по сезонам города {selected_city}",
            xaxis_title="Сезон",
            yaxis_title="Температура (°C)",
            hovermode="x unified"
        )

        st.plotly_chart(fig_season, use_container_width=True)

        st.divider()

        # Детальный график по месяцам
        st.subheader("Средняя температура по месяцам")
        city_df["month"] = city_df["timestamp"].dt.month
        monthly_stats = city_df.groupby("month")["temperature"].agg(
            mean="mean",
            std="std"
        ).reset_index()
        month_names = {
            1: "Янв", 2: "Фев", 3: "Мар", 4: "Апр",
            5: "Май", 6: "Июн", 7: "Июл", 8: "Авг",
            9: "Сен", 10: "Окт", 11: "Ноя", 12: "Дек"
        }
        monthly_stats["month_name"] = monthly_stats["month"].map(month_names)

        fig_monthly = go.Figure()
        fig_monthly.add_trace(go.Scatter(
            x=monthly_stats["month_name"],
            y=monthly_stats["mean"] + monthly_stats["std"],
            mode="lines",
            line=dict(width=0),
            showlegend=False
        ))

        fig_monthly.add_trace(go.Scatter(
            x=monthly_stats["month_name"],
            y=monthly_stats["mean"] - monthly_stats["std"],
            mode="lines",
            line=dict(color="green", width=0),
            fill="tonexty",
            name="Диапазон +-1",
            opacity=0.5

        ))

        # Линия среднего по месяцам
        fig_monthly.add_trace(go.Scatter(
            x=monthly_stats["month_name"],
            y=monthly_stats["mean"],
            mode="lines+markers",
            name="Средняя температура",
            line=dict(color="white", width=3),
            marker=dict(size=8)
        ))

        fig_monthly.update_layout(
            title=f"Среднемесячная температура города {selected_city}",
            xaxis_title="Месяц",
            yaxis_title="Температура (°C)",
            hovermode="x unified"
        )

        st.plotly_chart(fig_monthly, use_container_width=True)

    # Текущая погола
    with tab4:
        st.subheader(f"Текущая погода города {selected_city}")
        if not api_key:
            st.warning("Ошибка: введите API ключ OpenWeatherMap в боковой панели для получения текущей погоды.")

        else:
            curr_month = datetime.now().month
            curr_season = {
                12: "Зима", 1: "Зима", 2: "Зима",
                3: "Весна", 4: "Весна", 5: "Весна",
                6: "Лето", 7: "Лето", 8: "Лето",
                9: "Осень", 10: "Осень", 11: "Осень"
            }[curr_month]

            if st.button("Узнать"):
                weather = get_temp_sync(selected_city, api_key)
                if "error" in weather:
                    st.error(f"Ошибка API: {weather["error"]}")
                else:
                    curr_temp = weather["temperature"]
                    anomaly_result = check_temp_anomaly(curr_temp=curr_temp, city=selected_city,
                                                        season=curr_season, season_stats=city_season_stats)
                    st.divider()

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            label="Текущая температура",
                            value=f"{curr_temp:.1f} °C"
                        )
                    with col2:
                        st.metric(
                            label="Ощущается как",
                            value=f"{weather["feels_like"]:.1f} °C"
                        )
                    with col3:
                        st.metric(
                            label="Описание",
                            value=weather["description"].capitalize()
                        )

                    st.divider()

                    # Результат проверки на аномалность
                    st.subheader("Оценка температуры")

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            label="Норма сезона (среднее)",
                            value=f"{anomaly_result["season_mean"]} °C"
                        )
                    with col2:
                        st.metric(
                            label="Нижняя граница нормы",
                            value=f"{anomaly_result["lower"]} °C"
                        )
                    with col3:
                        st.metric(
                            label="Верхняя граница нормы",
                            value=f"{anomaly_result["upper"]} °C"
                        )

                    # Итоговый вывод
                    if anomaly_result["if_anomaly"]:
                        st.error(
                            f"Температура {curr_temp:.1f} °C является аномальной для сезона {curr_season}! "
                            f"Норма: {anomaly_result["lower"]} - {anomaly_result["upper"]} °C"
                        )
                    else:
                        st.success(
                            f"Температура {curr_temp:.1f} °C является нормальной для сезона {curr_season}. "
                            f"Норма: {anomaly_result["lower"]} - {anomaly_result["upper"]} °C"
                        )

                    st.divider()

                    # Пояснение про sync vs async
                    st.info(
                        """
                        **Вывод про синхронный и асинхронный запрос**

                        Для получения погоды одного города используем синхронный запрос (requests), потому что он 
                        проще и здесь нет разницы в скорости. Асинхронный вариант (aiohttp) лучше, когда нужно 
                        запросить много городов одновременно, тогда все запросы пойдут параллельно и не будут 
                        мешать/блокировать друг друга.
                        """
                    )
else:
    st.info("Загрузите файл с данными в боковой панели для того, чтобы начать.")
