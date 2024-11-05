import streamlit as st
import pandas as pd
import requests
import plotly.express as px
from kafka import KafkaConsumer
import json
from datetime import datetime

st.set_page_config(page_title="Weather Dashboard", page_icon=":sunny:", layout="wide", initial_sidebar_state="expanded")
NO_OF_DATA_POINTS = 15

def fetch_data():
    url = 'http://localhost:5000/api/data'
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data)

def sort_df_on_last_updated(df):
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    return df.sort_values(by='last_updated', ascending=False)

def apply_filters(df):    
    if(not apply):
        return df
    filtered_df = df.copy()
    
    timezone = st.sidebar.multiselect("Select TimeZone", filtered_df["timezone"].unique())
    if(timezone==[]):
        timezone = filtered_df["timezone"].unique()
    filtered_df = filtered_df[filtered_df["timezone"].isin(timezone)]
    location = st.sidebar.multiselect("Select Location", filtered_df["location_name"].unique())

    from_date = st.sidebar.date_input("From", min_value=filtered_df["last_updated"].min(), max_value=filtered_df["last_updated"].max(), value=filtered_df["last_updated"].min())
    to_date = st.sidebar.date_input("To", min_value=filtered_df["last_updated"].min(), max_value=filtered_df["last_updated"].max(), value=filtered_df["last_updated"].max())

    filtered_df = filtered_df.query("location_name == @location and last_updated >= @from_date and last_updated <= @to_date")
    filtered_df = filtered_df.sort_values(by='last_updated')
    return filtered_df

def plot_charts(filtered_df):
    if(apply):
        figures = [
            px.line(filtered_df, x='last_updated', y='temperature_celsius', color='location_name', title='Temperature Data'),
            px.line(filtered_df, x='last_updated', y='precip_mm', color='location_name', title='Precipitation Data'),
            px.line(filtered_df, x='last_updated', y='wind_kph', color='location_name', title='Wind Speed Data'),
            px.line(filtered_df, x='last_updated', y='humidity', color='location_name', title='Humidity Data')
        ]

        # Define how many plots to display per row
        plots_per_row = 2
        total_cols=  []
        # Display the plots in a grid layout
        for i in range(0, len(figures), plots_per_row):
            cols = st.columns(plots_per_row)
            total_cols.append(cols)
            for j, fig in enumerate(figures[i:i + plots_per_row]):
                with cols[j]:
                    st.plotly_chart(fig, use_container_width=True)
        return total_cols
    return None

def plot_live_data(df):
    # Plot temperature
    temp_fig = px.line(df, x='last_updated', y='temperature_celsius', title='Temperature (°C)')
    temp_plot_placeholder.plotly_chart(temp_fig, use_container_width=True)

    # Plot precipitation
    precip_fig = px.line(df, x='last_updated', y='precip_mm', title='Precipitation (mm)')
    precip_plot_placeholder.plotly_chart(precip_fig, use_container_width=True)

    # Plot wind speed
    wind_fig = px.line(df, x='last_updated', y='wind_kph', title='Wind Speed (kph)')
    wind_plot_placeholder.plotly_chart(wind_fig, use_container_width=True)

    # Plot humidity
    humidity_fig = px.line(df, x='last_updated', y='humidity', title='Humidity (%)')
    humidity_plot_placeholder.plotly_chart(humidity_fig, use_container_width=True)

st.sidebar.header("Weather Dashboard")
apply = st.sidebar.checkbox("Apply Filter")

df = fetch_data()
df['last_updated'] = pd.to_datetime(df['last_updated'])
df = sort_df_on_last_updated(df)
if('df' not in st.session_state):
    st.session_state.df = df
df = st.session_state.df
filtered_df = apply_filters(df)

# showing the Whole Database
st.title("Weather Data Dashboard")
weather_dashboard_all_data = st.empty()
weather_dashboard_all_data.dataframe(filtered_df)
plot_charts(filtered_df)

live_plot = st.sidebar.checkbox("Live Plot", value=False)

if 'live_data' not in st.session_state:
    st.session_state.live_data = pd.DataFrame(columns=df.columns)

if(live_plot):
    live_location = st.sidebar.multiselect("Select Location", df["location_name"].unique(),key="live_location")

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'global_weather',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    st.title("Live Weather Plots Streamed from KAFKA")
    
    # Create placeholders for the DataFrame and each plot
    data_placeholder = st.empty()
    total_cols=  []
    for i in range(2):
        cols=  st.columns(2)
        total_cols.append(cols)
    with total_cols[0][0]:
        temp_plot_placeholder = st.empty()
    with total_cols[0][1]:
        precip_plot_placeholder = st.empty()
    with total_cols[1][0]:
        wind_plot_placeholder = st.empty()
    with total_cols[1][1]:
        humidity_plot_placeholder = st.empty()
        
    data_placeholder.dataframe(st.session_state.live_data)
    filtered_on_location=  df.query("location_name == @live_location")
    filtered_on_location= sort_df_on_last_updated(filtered_on_location)
    filtered_on_location = filtered_on_location.head(NO_OF_DATA_POINTS)[::-1]
    plot_live_data(filtered_on_location)
    
    
    for message in consumer:
        new_data = message.value
        new_row = pd.DataFrame([new_data])
        print("Consuming data, ",datetime.now())
        new_row['last_updated'] = pd.to_datetime(new_row['last_updated'])
        st.session_state.live_data = pd.concat([st.session_state.live_data, new_row],ignore_index=True)
        st.session_state.df = pd.concat([st.session_state.df,new_row],ignore_index=True)
        live_data = st.session_state.live_data.reset_index(drop=True)
        data_placeholder.dataframe(st.session_state.live_data)
        st.session_state.df = sort_df_on_last_updated(st.session_state.df)
        st.session_state.df = st.session_state.df.reset_index(drop=True)
        weather_dashboard_all_data.dataframe(st.session_state.df)
        filtered_on_location=  st.session_state.df.query("location_name == @live_location")
        filtered_on_location= sort_df_on_last_updated(filtered_on_location)
        filtered_on_location = filtered_on_location.head(NO_OF_DATA_POINTS)[::-1]
        plot_live_data(filtered_on_location)
    
    

        


