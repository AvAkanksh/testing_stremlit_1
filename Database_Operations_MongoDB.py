from pymongo import MongoClient
import pandas as pd

# Load the CSV data into a DataFrame
df = pd.read_csv('weather.csv')

# Connect to the MongoDB server (ensure MongoDB is running locally or adjust connection details as needed)
client = MongoClient('mongodb://localhost:27017/')

# Create a database named 'WeatherDB' and a collection named 'GlobalWeatherData'
db = client['WeatherDB']
collection = db['GlobalWeather_data']

# Import the DataFrame into the MongoDB collection
data = df.to_dict('records')
collection.insert_many(data)
print("Data imported successfully into MongoDB!")

# Example: Find records from the month of May (using 'last_updated' field) or from a specific continent (e.g., "Africa")
month = 5
continent_countries = ["Algeria", "Angola"]  # List of countries in Africa

# Query to filter based on month from 'last_updated' field
records_by_month = collection.find({
    "$expr": { "$eq": [{ "$month": "$last_updated" }, month] }
})

# Query to filter based on continent (using the country field as a proxy)
records_by_continent = collection.find({
    "country": { "$in": continent_countries }
})

# Convert to list to display or process
records_by_month_list = list(records_by_month)
records_by_continent_list = list(records_by_continent)

# Print the results
print("Records from the month of May:")
for record in records_by_month_list:
    print(record)

print("\nRecords from Africa:")
for record in records_by_continent_list:
    print(record)