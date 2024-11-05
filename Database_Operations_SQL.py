from sqlalchemy import create_engine, select, desc, and_, func
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import MetaData, Table
import pandas as pd

# Database connection details
username = 'user'  # Replace with your MySQL username
password = 'password'      # Replace with your MySQL password
host = 'localhost' # Replace with your MySQL host, e.g., 'localhost'
port = '3306'      # Replace with your MySQL port, default is 3306
database = 'GlobalWeatherDB'  # The database you're connecting to
socket_path = '/var/run/mysqld/mysqld.sock'

# Create the connection string
connection_string = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}?unix_socket={socket_path}'

# Create an SQLAlchemy engine
engine = create_engine(connection_string)
df = pd.read_csv('weather.csv')
df.to_sql('weather_data_cleaned', con=engine, if_exists='replace', index=False)
print("Data inserted successfully!")

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Reflect existing database structure
metadata = MetaData()
weather_data_cleaned = Table('weather_data_cleaned', metadata, autoload_with=engine)

# 1. Retrieve the top 5 locations with the highest temperature
query_top5 = select(
    weather_data_cleaned.c.location_name,
    weather_data_cleaned.c.temperature_celsius,
).order_by(
    desc(weather_data_cleaned.c.temperature_celsius)).limit(5)

results_top5 = session.execute(query_top5).fetchall()
print("Top 5 locations with highest temperatures:")
for row in results_top5:
    print(row)
    

# 2. Retrieve all records for a specific date or condition (e.g., temperature > 35°C, precipitation > 100 mm).
query_specific_conditions = select(
    weather_data_cleaned
).where(
    and_(
        weather_data_cleaned.c.temperature_celsius > 35,
        weather_data_cleaned.c.precip_mm > 100
    )
)

results_conditions = session.execute(query_specific_conditions).fetchall()
print("\nRecords with temperature > 35°C and precipitation > 100 mm:")
for row in results_conditions:
    print(row)

# 3. Group by operation (e.g., average temperature by country).
query_group_by_avg_temp = select(
    weather_data_cleaned.c.country,
    func.avg(weather_data_cleaned.c.temperature_celsius).label('avg_temperature')
).group_by(
    weather_data_cleaned.c.country
)

results_avg_temp = session.execute(query_group_by_avg_temp).fetchall()
print("\nAverage temperature by country:")
for row in results_avg_temp:
    print(row)

# 4. Group by operation (e.g., total precipitation by location_name).
query_group_by_total_precip = select(
    weather_data_cleaned.c.location_name,
    func.sum(weather_data_cleaned.c.precip_mm).label('total_precipitation')
).group_by(
    weather_data_cleaned.c.location_name
)

results_total_precip = session.execute(query_group_by_total_precip).fetchall()
print("\nTotal precipitation by Location:")
for country, total_precip in results_total_precip:
    print("Country: ", country, "| Total Precipitation: ", total_precip)


session.close()
