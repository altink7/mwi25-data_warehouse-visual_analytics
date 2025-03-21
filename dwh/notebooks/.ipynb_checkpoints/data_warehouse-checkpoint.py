#%% md
# # Data Warehouse Citibike
#%% md
# ## Project Info
# 
# Group: **Real estate**
# 
# Team Members: **Altin Kelmendi, Julian Hoffmann, Daniel Dodmasej, Clemens Hohensinner**
# 
# 
# 
#%% md
# ## Setup &  Imports
#%%
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Interval, ForeignKey, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker

# Database connection string (update as needed)
DATABASE_URL = 'postgresql://root:root@localhost:5452/citibike_db'

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()


#%% md
# ## Table Definitions ( SQLAlchemy ORM)
# 
#%%
class UserType(Base):
    __tablename__ = 'usertype'
    userTypeId = Column(Integer, primary_key=True, autoincrement=True)
    userType = Column(String(50), nullable=False)
    description = Column(String(250))


class Station(Base):
    __tablename__ = 'station'
    id = Column(String(50), primary_key=True)
    name = Column(String(50), nullable=False)
    oldName = Column(String(50), nullable=True)


class Date(Base):
    __tablename__ = 'date'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tripId = Column(String(50), nullable=False)
    hour = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    minute = Column(Integer, nullable=False)
    dateTime = Column(TIMESTAMP, nullable=False)


class FactTrip(Base):
    __tablename__ = 'fact_trip'
    tripId = Column(String(50), primary_key=True)
    startStation = Column(String(50), ForeignKey('station.id'), nullable=False)
    stopStation = Column(String(50), ForeignKey('station.id'), nullable=False)
    stopTime = Column(TIMESTAMP, nullable=False)
    userTypeId = Column(Integer, ForeignKey('usertype.userTypeId'), nullable=False)
    startTime = Column(TIMESTAMP, nullable=False)
    duration = Column(Interval)
    dateId = Column(Integer, ForeignKey('date.id'), nullable=False)


Base.metadata.create_all(engine)

#%% md
# ##  Load CSV Files (Extraction Phase)
# 
#%%
import pandas as pd

df_december = pd.read_csv('CitiBike_December.csv')
df_january = pd.read_csv('CitiBike_January.csv')
df_february = pd.read_csv('CitiBike_February.csv')
df_202501 = pd.read_csv('202501.csv')
df_202502 = pd.read_csv('202502.csv')

# Combines DataFrames
df_all = pd.concat([df_december, df_january, df_february, df_202501, df_202502], ignore_index=True)

print(f"Combined data shape: {df_all.shape}")
df_all.head()

#%% md
# ## Data Cleansing & Transformation
# 
#%%
# 1:  duplicates
df_all.drop_duplicates(inplace=True)

# 2 : null values
df_all.dropna(subset=['ride_id', 'started_at', 'ended_at', 'start_station_id', 'end_station_id', 'member_casual'],
              inplace=True)

# 3:  data types
df_all['started_at'] = pd.to_datetime(df_all['started_at'])
df_all['ended_at'] = pd.to_datetime(df_all['ended_at'])

# 4:  duration
df_all['duration'] = df_all['ended_at'] - df_all['started_at']

# 5:  date dimension
df_all['hour'] = df_all['started_at'].dt.hour
df_all['day'] = df_all['started_at'].dt.day
df_all['month'] = df_all['started_at'].dt.month
df_all['year'] = df_all['started_at'].dt.year
df_all['minute'] = df_all['started_at'].dt.minute

# 6:  station dimension
date_df = df_all[['ride_id', 'hour', 'day', 'month', 'year', 'minute', 'started_at']].copy()
date_df.rename(columns={'started_at': 'dateTime', 'ride_id': 'tripId'}, inplace=True)

#%% md
# ## Load UserType Dimension
# 
# 
#%%
usertype_map = {
    'member': ('Member', 'Registered user'),
    'casual': ('Casual', 'Unregistered user')
}

user_types = pd.DataFrame(usertype_map).T.reset_index()
user_types.columns = ['member_casual', 'userType', 'description']

# Load
for _, row in user_types.iterrows():
    user_type_entry = UserType(userType=row['userType'], description=row['description'])
    session.merge(user_type_entry)
session.commit()

user_type_lookup = session.query(UserType).all()
user_type_dict = {ut.userType.lower(): ut.userTypeId for ut in user_type_lookup}
df_all['userTypeId'] = df_all['member_casual'].map(lambda x: user_type_dict.get(usertype_map[x][0].lower()))

#%% md
# ## Load Station Dimension
# 
#%%
#  unique stations
start_stations = df_all[['start_station_id', 'start_station_name']].drop_duplicates()
start_stations.columns = ['id', 'name']
stop_stations = df_all[['end_station_id', 'end_station_name']].drop_duplicates()
stop_stations.columns = ['id', 'name']

stations = pd.concat([start_stations, stop_stations]).drop_duplicates(subset='id')

for _, row in stations.iterrows():
    station_entry = Station(id=str(row['id']), name=row['name'])
    session.merge(station_entry)
session.commit()

#%% md
# ## Load Date Dimension
#%%
date_id_map = {}
for _, row in date_df.iterrows():
    date_entry = Date(
        tripId=row['tripId'],
        hour=row['hour'],
        day=row['day'],
        month=row['month'],
        year=row['year'],
        minute=row['minute'],
        dateTime=row['dateTime']
    )
    session.add(date_entry)
    session.flush()
    date_id_map[row['tripId']] = date_entry.id
session.commit()

#%% md
# ## Load Fact Table (FactTrip)
#%%
df_all['dateId'] = df_all['ride_id'].map(date_id_map)

for _, row in df_all.iterrows():
    trip_entry = FactTrip(
        tripId=row['ride_id'],
        startStation=str(row['start_station_id']),
        stopStation=str(row['end_station_id']),
        stopTime=row['ended_at'],
        userTypeId=row['userTypeId'],
        startTime=row['started_at'],
        duration=row['duration'],
        dateId=row['dateId']
    )
    session.merge(trip_entry)
session.commit()

#%% md
# ## Notes:
#%%
