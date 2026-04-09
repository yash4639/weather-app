"""
Weather Data App (Streamlit + SQL Server + OpenWeather)
-------------------------------------------------------

This app lets you:
  - Select a city from dropdown
  - Fetch live weather data using OpenWeather API
  - Insert that data into SQL Server (table: weather_data)
  - Display fetched data beautifully on Streamlit UI

Usage:
  streamlit run main.py

Make sure:
  - OPENWEATHER_API_KEY is valid
  - SQL Server connection details are correct
  - Table 'dbo.weather_data' exists (auto-created if missing)
"""

import os
import json
import datetime
import asyncio
import httpx
import pyodbc
import bcrypt
import streamlit as st

# -------------------------
# Configuration
# -------------------------
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

SQL_SERVER_CONFIG = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "YASH\\SQLEXPRESS",
    "database": "WeatherDB",
    "trusted_connection": True
}

UNITS = "metric"  # Celsius

# -------------------------
# City list
# -------------------------
CITIES = [
    {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"city": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"city": "Berlin", "country": "DE", "lat": 52.5200, "lon": 13.4050},
    {"city": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"city": "Mumbai", "country": "IN", "lat": 19.0760, "lon": 72.8777},
    {"city": "Delhi", "country": "IN", "lat": 28.7041, "lon": 77.1025},
    {"city": "Bangalore", "country": "IN", "lat": 12.9716, "lon": 77.5946},
    {"city": "Chennai", "country": "IN", "lat": 13.0827, "lon": 80.2707},
    {"city": "Sydney", "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "Pune", "country": "IN", "lat": 18.5204, "lon": 73.8867},    
    {"city": "Nagpur", "country": "IN", "lat":21.1500, "lon": 79.0900},    

]

# -------------------------
# Database functions
# -------------------------
def get_connection_string(cfg):
    if cfg.get("trusted_connection"):
        return f"DRIVER={{{cfg['driver']}}};SERVER={cfg['server']};DATABASE={cfg['database']};Trusted_Connection=yes"
    else:
        return f"DRIVER={{{cfg['driver']}}};SERVER={cfg['server']};DATABASE={cfg['database']};UID={cfg['username']};PWD={cfg['password']}"

def ensure_database_and_table(cfg):
    master_cfg = cfg.copy()
    master_cfg["database"] = "master"
    conn_str_master = get_connection_string(master_cfg)
    with pyodbc.connect(conn_str_master, autocommit=True) as conn:
        cur = conn.cursor()
        dbname = cfg["database"]
        cur.execute(f"""
        IF DB_ID(N'{dbname}') IS NULL
        BEGIN
            CREATE DATABASE [{dbname}];
        END
        """)
    conn_str_db = get_connection_string(cfg)
    with pyodbc.connect(conn_str_db, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(f"""
        IF OBJECT_ID('dbo.weather_data','U') IS NULL
        BEGIN
            CREATE TABLE dbo.weather_data (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                city NVARCHAR(128),
                country NVARCHAR(8),
                lat FLOAT,
                lon FLOAT,
                forecast_ts_utc DATETIME2,
                fetched_at_utc DATETIME2,
                temp_c FLOAT,
                feels_like_c FLOAT,
                humidity INT,
                pressure INT,
                wind_speed FLOAT,
                weather_main NVARCHAR(64),
                weather_description NVARCHAR(128),
                raw_payload NVARCHAR(MAX)
            );
        END
        """)
    return True

def insert_weather_row(cfg, row):
    conn_str_db = get_connection_string(cfg)
    with pyodbc.connect(conn_str_db, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO dbo.weather_data
            (city, country, lat, lon, forecast_ts_utc, fetched_at_utc,
             temp_c, feels_like_c, humidity, pressure, wind_speed,
             weather_main, weather_description, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("city"),
            row.get("country"),
            row.get("lat"),
            row.get("lon"),
            row.get("forecast_ts_utc"),
            row.get("fetched_at_utc"),
            row.get("temp_c"),
            row.get("feels_like_c"),
            row.get("humidity"),
            row.get("pressure"),
            row.get("wind_speed"),
            row.get("weather_main"),
            row.get("weather_description"),
            json.dumps(row.get("raw_payload"), ensure_ascii=False)
        ))

# -------------------------
# Weather fetch function
# -------------------------
async def fetch_weather(lat, lon):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": UNITS}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, timeout=20.0)
        r.raise_for_status()
        data = r.json()
    return data

# -------------------------
# Streamlit App UI
# -------------------------
st.set_page_config(page_title="Global Weather Fetcher", page_icon="🌦️", layout="centered")

st.title("🌦️ Global Weather Fetcher")
st.write("Fetch and store live weather data for selected cities into your SQL Server database.")

# Initialize session
if "last_city" not in st.session_state:
    st.session_state["last_city"] = None
if "weather_data" not in st.session_state:
    st.session_state["weather_data"] = None

# Dropdown for city
city_names = [c["city"] for c in CITIES]
selected_city_name = st.selectbox("Select a city to fetch weather data:", city_names)

# Button to fetch
if st.button("Fetch & Save Weather Data"):
    selected_city = next(c for c in CITIES if c["city"] == selected_city_name)
    st.session_state["last_city"] = selected_city_name
    try:
        with st.spinner(f"Fetching weather data for {selected_city_name}..."):
            data = asyncio.run(fetch_weather(selected_city["lat"], selected_city["lon"]))
            fetched_at = datetime.datetime.utcnow()

            # Prepare row for DB
            row = {
                "city": selected_city["city"],
                "country": selected_city["country"],
                "lat": selected_city["lat"],
                "lon": selected_city["lon"],
                "forecast_ts_utc": datetime.datetime.utcfromtimestamp(data["dt"]),
                "fetched_at_utc": fetched_at,
                "temp_c": data["main"]["temp"],
                "feels_like_c": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data["wind"]["speed"],
                "weather_main": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "raw_payload": data
            }

            # Ensure DB + insert
            ensure_database_and_table(SQL_SERVER_CONFIG)
            insert_weather_row(SQL_SERVER_CONFIG, row)

            st.session_state["weather_data"] = row

            st.success(f"✅ Data fetched & saved successfully for {selected_city_name}!")

    except Exception as e:
        st.error(f"❌ Error: {e}")

# Show results
if st.session_state["weather_data"]:
    w = st.session_state["weather_data"]
    st.subheader(f"🌍 Weather in {w['city']}, {w['country']}")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("🌡️ Temperature (°C)", f"{w['temp_c']}°C")
        st.metric("💨 Wind Speed", f"{w['wind_speed']} m/s")
        st.metric("💧 Humidity", f"{w['humidity']}%")
    with col2:
        st.metric("🎯 Feels Like", f"{w['feels_like_c']}°C")
        st.metric("📈 Pressure", f"{w['pressure']} hPa")
        st.metric("☁️ Condition", w["weather_description"].title())

    with st.expander("🔍 View Raw API Data"):
        st.json(w["raw_payload"])