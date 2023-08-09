import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from models import Base

print("creating tables")

# Read CSV files
activity_df = pd.read_csv("./dataCsv/store_status.csv")
business_hours_df = pd.read_csv("./dataCsv/menu_hours.csv")
timezones_df = pd.read_csv("./dataCsv/time_zone.csv")

# Set up SQLite database
db_engine = create_engine('sqlite:///restaurant_data.db')


Base.metadata.create_all(db_engine)

# Store dataframes in the database
activity_df.to_sql('activity', db_engine, if_exists='replace', index=False)
business_hours_df.to_sql('business_hours', db_engine, if_exists='replace', index=False)
timezones_df.to_sql('time_zones', db_engine, if_exists='replace', index=False)

Session = sessionmaker(bind=db_engine)
session = Session()