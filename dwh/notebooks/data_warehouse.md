# Data Warehouse Citibike

## Project Info

Group: **Real estate**

Team Members: **Altin Kelmendi, Julian Hoffmann, Daniel Dodmasej, Clemens Hohensinner**




## Setup &  Imports


```python
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Interval, ForeignKey, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker

# DB from Docker (Postgres) / run docker-compose up before
DATABASE_URL = 'postgresql://root:root@localhost:5452/citibike_db'

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()


```

## Table Definitions ( SQLAlchemy ORM)



```python
class UserType(Base):
    __tablename__ = 'usertype'
    __table_args__ = {'extend_existing': True}

    usertypeid = Column(Integer, primary_key=True, autoincrement=True)
    usertype = Column(String(50), nullable=False)
    description = Column(String(250))


class Station(Base):
    __tablename__ = 'station'
    __table_args__ = {'extend_existing': True}

    id = Column(String(50), primary_key=True)
    name = Column(String(50), nullable=False)
    oldname = Column(String(50), nullable=True)


class Date(Base):
    __tablename__ = 'date'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    hour = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    minute = Column(Integer, nullable=False)
    datetime = Column(TIMESTAMP, nullable=False)


class FactTrip(Base):
    __tablename__ = 'fact_trip'
    __table_args__ = {'extend_existing': True}

    tripid = Column(String(50), primary_key=True)
    startstation = Column(String(50), ForeignKey('station.id'), nullable=False)
    stopstation = Column(String(50), ForeignKey('station.id'), nullable=False)
    starttimeid = Column(Integer, ForeignKey('date.id'), nullable=False)
    stoptimeid = Column(Integer, ForeignKey('date.id'), nullable=False)
    usertypeid = Column(Integer, ForeignKey('usertype.usertypeid'), nullable=False)
    duration = Column(Interval)


Base.metadata.create_all(engine)

```

    /var/folders/0p/753q2f_x1x11hw9t93264c2r0000gn/T/ipykernel_62903/2014356508.py:1: SAWarning: This declarative base already contains a class with the same class name and module name as __main__.UserType, and will be replaced in the string-lookup table.
      class UserType(Base):
    /var/folders/0p/753q2f_x1x11hw9t93264c2r0000gn/T/ipykernel_62903/2014356508.py:10: SAWarning: This declarative base already contains a class with the same class name and module name as __main__.Station, and will be replaced in the string-lookup table.
      class Station(Base):
    /var/folders/0p/753q2f_x1x11hw9t93264c2r0000gn/T/ipykernel_62903/2014356508.py:19: SAWarning: This declarative base already contains a class with the same class name and module name as __main__.Date, and will be replaced in the string-lookup table.
      class Date(Base):
    /var/folders/0p/753q2f_x1x11hw9t93264c2r0000gn/T/ipykernel_62903/2014356508.py:32: SAWarning: This declarative base already contains a class with the same class name and module name as __main__.FactTrip, and will be replaced in the string-lookup table.
      class FactTrip(Base):


##  Load CSV Files (Extraction Phase)



```python
import pandas as pd

main_path = '../data/content/'

df_december = pd.read_csv(main_path + '202412-tripdata.csv')
df_january = pd.read_csv(main_path + '202501-tripdata.csv')
df_february = pd.read_csv(main_path + '202502-tripdata.csv')

# Combines DataFrames
df_all = pd.concat([df_december, df_january, df_february], ignore_index=True)

print(f"Combined data shape: {df_all.shape}")
df_all.head()

```

    Combined data shape: (150699, 13)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ride_id</th>
      <th>rideable_type</th>
      <th>started_at</th>
      <th>ended_at</th>
      <th>start_station_name</th>
      <th>start_station_id</th>
      <th>end_station_name</th>
      <th>end_station_id</th>
      <th>start_lat</th>
      <th>start_lng</th>
      <th>end_lat</th>
      <th>end_lng</th>
      <th>member_casual</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>28A17ACD224CD80B</td>
      <td>electric_bike</td>
      <td>2024-12-06 17:50:49.428</td>
      <td>2024-12-06 17:54:20.070</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Hilltop</td>
      <td>JC019</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>40.731169</td>
      <td>-74.057574</td>
      <td>member</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3508393A86FBD357</td>
      <td>classic_bike</td>
      <td>2024-12-14 11:01:00.309</td>
      <td>2024-12-14 11:12:01.382</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Hoboken Terminal - Hudson St &amp; Hudson Pl</td>
      <td>HB101</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>40.735938</td>
      <td>-74.030305</td>
      <td>member</td>
    </tr>
    <tr>
      <th>2</th>
      <td>75FA4C03A1447401</td>
      <td>electric_bike</td>
      <td>2024-12-24 08:07:17.475</td>
      <td>2024-12-24 08:14:14.612</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Leonard Gordon Park</td>
      <td>JC080</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>40.745910</td>
      <td>-74.057271</td>
      <td>member</td>
    </tr>
    <tr>
      <th>3</th>
      <td>C7741EF495C597DD</td>
      <td>classic_bike</td>
      <td>2024-12-19 12:48:05.452</td>
      <td>2024-12-19 12:54:15.253</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Leonard Gordon Park</td>
      <td>JC080</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>40.745910</td>
      <td>-74.057271</td>
      <td>member</td>
    </tr>
    <tr>
      <th>4</th>
      <td>07952BB20B46C5B1</td>
      <td>electric_bike</td>
      <td>2024-12-17 11:19:37.631</td>
      <td>2024-12-17 11:28:25.150</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Grove St PATH</td>
      <td>JC115</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>40.719410</td>
      <td>-74.043090</td>
      <td>casual</td>
    </tr>
  </tbody>
</table>
</div>



## Data Cleansing & Transformation



```python
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
# Combine started_at and ended_at into one date dataframe
combined_dates = pd.concat([
    df_all[['started_at']].rename(columns={'started_at': 'dateTime'}),
    df_all[['ended_at']].rename(columns={'ended_at': 'dateTime'})
]).drop_duplicates()

df_all['hour'] = df_all['started_at'].dt.hour
df_all['day'] = df_all['started_at'].dt.day
df_all['month'] = df_all['started_at'].dt.month
df_all['year'] = df_all['started_at'].dt.year
df_all['minute'] = df_all['started_at'].dt.minute

# 6:  station dimension
date_df = df_all[['ride_id', 'hour', 'day', 'month', 'year', 'minute', 'started_at']].copy()
date_df.rename(columns={'started_at': 'dateTime', 'ride_id': 'tripid'}, inplace=True)
date_df.drop_duplicates(subset=['dateTime'], inplace=True)

# 7: cleaned data
print(f"Cleaned data shape: {df_all.shape}")
df_all.head()

```

    Cleaned data shape: (150274, 23)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ride_id</th>
      <th>rideable_type</th>
      <th>started_at</th>
      <th>ended_at</th>
      <th>start_station_name</th>
      <th>start_station_id</th>
      <th>end_station_name</th>
      <th>end_station_id</th>
      <th>start_lat</th>
      <th>start_lng</th>
      <th>...</th>
      <th>duration</th>
      <th>hour</th>
      <th>day</th>
      <th>month</th>
      <th>year</th>
      <th>minute</th>
      <th>userTypeId</th>
      <th>usertypeid</th>
      <th>startdateid</th>
      <th>stopdateid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>28A17ACD224CD80B</td>
      <td>electric_bike</td>
      <td>2024-12-06 17:50:49.428</td>
      <td>2024-12-06 17:54:20.070</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Hilltop</td>
      <td>JC019</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>...</td>
      <td>0 days 00:03:30.642000</td>
      <td>17</td>
      <td>6</td>
      <td>12</td>
      <td>2024</td>
      <td>50</td>
      <td>1</td>
      <td>NaN</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3508393A86FBD357</td>
      <td>classic_bike</td>
      <td>2024-12-14 11:01:00.309</td>
      <td>2024-12-14 11:12:01.382</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Hoboken Terminal - Hudson St &amp; Hudson Pl</td>
      <td>HB101</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>...</td>
      <td>0 days 00:11:01.073000</td>
      <td>11</td>
      <td>14</td>
      <td>12</td>
      <td>2024</td>
      <td>1</td>
      <td>1</td>
      <td>NaN</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>75FA4C03A1447401</td>
      <td>electric_bike</td>
      <td>2024-12-24 08:07:17.475</td>
      <td>2024-12-24 08:14:14.612</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Leonard Gordon Park</td>
      <td>JC080</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>...</td>
      <td>0 days 00:06:57.137000</td>
      <td>8</td>
      <td>24</td>
      <td>12</td>
      <td>2024</td>
      <td>7</td>
      <td>1</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>C7741EF495C597DD</td>
      <td>classic_bike</td>
      <td>2024-12-19 12:48:05.452</td>
      <td>2024-12-19 12:54:15.253</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Leonard Gordon Park</td>
      <td>JC080</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>...</td>
      <td>0 days 00:06:09.801000</td>
      <td>12</td>
      <td>19</td>
      <td>12</td>
      <td>2024</td>
      <td>48</td>
      <td>1</td>
      <td>NaN</td>
      <td>4</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>07952BB20B46C5B1</td>
      <td>electric_bike</td>
      <td>2024-12-17 11:19:37.631</td>
      <td>2024-12-17 11:28:25.150</td>
      <td>Oakland Ave</td>
      <td>JC022</td>
      <td>Grove St PATH</td>
      <td>JC115</td>
      <td>40.737604</td>
      <td>-74.052478</td>
      <td>...</td>
      <td>0 days 00:08:47.519000</td>
      <td>11</td>
      <td>17</td>
      <td>12</td>
      <td>2024</td>
      <td>19</td>
      <td>2</td>
      <td>NaN</td>
      <td>5</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 23 columns</p>
</div>



## Load UserType Dimension




```python
# Recreate session (rollback any previous failed session)
session.rollback()

usertype_map = {
    'member': ('Member', 'Registered user'),
    'casual': ('Casual', 'Unregistered user')
}

# Create DataFrame from mapping
user_types_df = pd.DataFrame(usertype_map).T.reset_index()
user_types_df.columns = ['member_casual', 'usertype', 'description']

# Insert or update user types into the DB
for _, row in user_types_df.iterrows():
    user_type_entry = UserType(usertype=row['usertype'], description=row['description'])
    session.merge(user_type_entry)

session.commit()

# Fetch all user types from DB and map usertype (lowercase) to usertypeid
user_type_lookup = session.query(UserType).all()
user_type_dict = {ut.usertype.lower(): ut.usertypeid for ut in user_type_lookup}

# Map 'member_casual' to userTypeId
df_all['userTypeId'] = df_all['member_casual'].map(lambda x: user_type_dict.get(usertype_map[x][0].lower()))

```

## Load Station Dimension



```python
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

```

## Load Date Dimension


```python
date_id_map = {}

for _, row in date_df.iterrows():
    dt = row['dateTime']

    if dt not in date_id_map:
        date_entry = Date(
            hour=row['hour'],
            day=row['day'],
            month=row['month'],
            year=row['year'],
            minute=row['minute'],
            datetime=dt
        )
        session.add(date_entry)
        session.flush()
        date_id_map[dt] = date_entry.id

session.commit()

```

## Load Fact Table (FactTrip)


```python
print(df_all.columns.tolist())
print(df_all.head())

# Correct timestamp mapping for both start and end times
df_all['startdateid'] = df_all['started_at'].map(date_id_map)
df_all['stopdateid'] = df_all['ended_at'].map(date_id_map)

try:
    for _, row in df_all.iterrows():
        if pd.isnull(row['startdateid']) or pd.isnull(row['stopdateid']):
            print(f"Skipping row with missing dateid: {row['ride_id']} - {row['startdateid']} - {row['stopdateid']}")
            continue

        trip_entry = FactTrip(
            tripid=row['ride_id'],
            startstation=str(row['start_station_id']),
            stopstation=str(row['end_station_id']),
            usertypeid=row['userTypeid'],  # FIXED this line
            duration=row['duration'],
            startdateid=int(row['startdateid']),
            stopdateid=int(row['stopdateid'])
        )
        session.merge(trip_entry)

    session.commit()

except Exception as e:
    session.rollback()
    print(f"Error occurred: {e}")

```

    ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual', 'duration', 'hour', 'day', 'month', 'year', 'minute', 'userTypeId', 'usertypeid', 'startdateid', 'stopdateid']
                ride_id  rideable_type              started_at  \
    0  28A17ACD224CD80B  electric_bike 2024-12-06 17:50:49.428   
    1  3508393A86FBD357   classic_bike 2024-12-14 11:01:00.309   
    2  75FA4C03A1447401  electric_bike 2024-12-24 08:07:17.475   
    3  C7741EF495C597DD   classic_bike 2024-12-19 12:48:05.452   
    4  07952BB20B46C5B1  electric_bike 2024-12-17 11:19:37.631   
    
                     ended_at start_station_name start_station_id  \
    0 2024-12-06 17:54:20.070        Oakland Ave            JC022   
    1 2024-12-14 11:12:01.382        Oakland Ave            JC022   
    2 2024-12-24 08:14:14.612        Oakland Ave            JC022   
    3 2024-12-19 12:54:15.253        Oakland Ave            JC022   
    4 2024-12-17 11:28:25.150        Oakland Ave            JC022   
    
                               end_station_name end_station_id  start_lat  \
    0                                   Hilltop          JC019  40.737604   
    1  Hoboken Terminal - Hudson St & Hudson Pl          HB101  40.737604   
    2                       Leonard Gordon Park          JC080  40.737604   
    3                       Leonard Gordon Park          JC080  40.737604   
    4                             Grove St PATH          JC115  40.737604   
    
       start_lng  ...               duration  hour day month  year  minute  \
    0 -74.052478  ... 0 days 00:03:30.642000    17   6    12  2024      50   
    1 -74.052478  ... 0 days 00:11:01.073000    11  14    12  2024       1   
    2 -74.052478  ... 0 days 00:06:57.137000     8  24    12  2024       7   
    3 -74.052478  ... 0 days 00:06:09.801000    12  19    12  2024      48   
    4 -74.052478  ... 0 days 00:08:47.519000    11  17    12  2024      19   
    
       userTypeId  usertypeid  startdateid  stopdateid  
    0           1         NaN            1         NaN  
    1           1         NaN            2         NaN  
    2           1         NaN            3         NaN  
    3           1         NaN            4         NaN  
    4           2         NaN            5         NaN  
    
    [5 rows x 23 columns]
    Skipping row with missing dateid: 28A17ACD224CD80B - 150272 - nan
    Skipping row with missing dateid: 3508393A86FBD357 - 150273 - nan
    Skipping row with missing dateid: 75FA4C03A1447401 - 150274 - nan
    Skipping row with missing dateid: C7741EF495C597DD - 150275 - nan
    Skipping row with missing dateid: 07952BB20B46C5B1 - 150276 - nan
    Skipping row with missing dateid: 431601566E33CCB7 - 150277 - nan
    Skipping row with missing dateid: FB052BAFC90AF0AB - 150278 - nan
    Skipping row with missing dateid: 98239E90D8714A23 - 150279 - nan
    Skipping row with missing dateid: 505BF82173A81F99 - 150280 - nan
    Skipping row with missing dateid: A0DAEB761EC2ADD1 - 150281 - nan
    Skipping row with missing dateid: 4072A4A809AA4682 - 150282 - nan
    Skipping row with missing dateid: 064643EE507D9CCE - 150283 - nan
    Skipping row with missing dateid: C0B4AD5D60FC5814 - 150284 - nan
    Skipping row with missing dateid: F8E43C510CAB81FA - 150285 - nan
    Skipping row with missing dateid: EBD8A20488BF4E29 - 150286 - nan
    Skipping row with missing dateid: 572149683292D858 - 150287 - nan
    Skipping row with missing dateid: D73D5AE5D8E0FFFF - 150288 - nan
    Skipping row with missing dateid: 1B8091ACEB79F7CF - 150289 - nan
    Skipping row with missing dateid: 044A0CC29340011E - 150290 - nan
    Skipping row with missing dateid: F1175F5F9823FB04 - 150291 - nan
    Skipping row with missing dateid: 5F4352D7567F3149 - 150292 - nan
    Skipping row with missing dateid: ED64F09FAF1098B2 - 150293 - nan
    Skipping row with missing dateid: 09AAD5029CAFDA5C - 150294 - nan
    Skipping row with missing dateid: 2EA8D9B296D963FA - 150295 - nan
    Skipping row with missing dateid: 54814FC1E32999A8 - 150296 - nan
    Skipping row with missing dateid: 3B72548611532846 - 150297 - nan
    Skipping row with missing dateid: 8AA62509433CA8FB - 150298 - nan
    Skipping row with missing dateid: 1D0FFDF41A4B20D4 - 150299 - nan
    Skipping row with missing dateid: 2D8E5C35CA737010 - 150300 - nan
    Skipping row with missing dateid: 1EC4DDFA9BF0A3C5 - 150301 - nan
    Skipping row with missing dateid: 6D4905E7A46604E0 - 150302 - nan
    Skipping row with missing dateid: 834BE406F3462141 - 150303 - nan
    Skipping row with missing dateid: D06FF6C656046198 - 150304 - nan
    Skipping row with missing dateid: ADE47C79357E97B9 - 150305 - nan
    Skipping row with missing dateid: 8DA6331CB202E51D - 150306 - nan
    Skipping row with missing dateid: 4F027965DDEC9D5F - 150307 - nan
    Skipping row with missing dateid: B73D93FD2F9B8649 - 150308 - nan
    Skipping row with missing dateid: 391934469E1AA302 - 150309 - nan
    Skipping row with missing dateid: B81154A0C758E19F - 150310 - nan
    Skipping row with missing dateid: DF3018BC21E923A6 - 150311 - nan
    Skipping row with missing dateid: F0F32941892E4028 - 150312 - nan
    Skipping row with missing dateid: A21EF263DE182B4D - 150313 - nan
    Skipping row with missing dateid: 0D15A4B0F69E6524 - 150314 - nan
    Skipping row with missing dateid: 62EBE8665023B0F4 - 150315 - nan
    Skipping row with missing dateid: A517B6028D2A2A12 - 150316 - nan
    Skipping row with missing dateid: 182D8C4E05CB163B - 150317 - nan
    Skipping row with missing dateid: 99D2BD803A4179E5 - 150318 - nan
    Skipping row with missing dateid: 08AD01CDDB935872 - 150319 - nan
    Skipping row with missing dateid: A853CBDC63A8A27E - 150320 - nan
    Skipping row with missing dateid: 7429131A3896E054 - 150321 - nan
    Skipping row with missing dateid: E0429AF0FEC976BB - 150322 - nan
    Skipping row with missing dateid: B35AF9FECDBDECE2 - 150323 - nan
    Skipping row with missing dateid: F425A2EA6252DB0F - 150324 - nan
    Skipping row with missing dateid: C43E82B5607524A8 - 150325 - nan
    Skipping row with missing dateid: E17A619004E1ECB4 - 150326 - nan
    Skipping row with missing dateid: 5DBE9C9D124135DE - 150327 - nan
    Skipping row with missing dateid: 9C5F83F5034261AB - 150328 - nan
    Skipping row with missing dateid: 7ACCC6E340EFEB10 - 150329 - nan
    Skipping row with missing dateid: 9F9CC8F079565A8C - 150330 - nan
    Skipping row with missing dateid: 98FFAC06DAFB2704 - 150331 - nan
    Skipping row with missing dateid: 799C37AD180A352F - 150332 - nan
    Skipping row with missing dateid: 391DF75DDCBAE829 - 150333 - nan
    Skipping row with missing dateid: 440E34BAE4348870 - 150334 - nan
    Skipping row with missing dateid: 232C0BAF47429BDC - 150335 - nan
    Skipping row with missing dateid: 7F80DB42BB5657C0 - 150336 - nan
    Skipping row with missing dateid: 769E6F54BD6B82CE - 150337 - nan
    Skipping row with missing dateid: 6F0A6C70C3CB58E7 - 150338 - nan
    Skipping row with missing dateid: AB1C3960513F5967 - 150339 - nan
    Skipping row with missing dateid: 1C973829456CFB8B - 150340 - nan
    Skipping row with missing dateid: 7B5E31DE338846F9 - 150341 - nan
    Skipping row with missing dateid: 9D0D5A549B8256C3 - 150342 - nan
    Skipping row with missing dateid: 8D1525C0CFD91C48 - 150343 - nan
    Skipping row with missing dateid: 4400641F5DFE17D6 - 150344 - nan
    Skipping row with missing dateid: 96ED386238A82142 - 150345 - nan
    Skipping row with missing dateid: 6B0E407F6C9C7238 - 150346 - nan
    Skipping row with missing dateid: 1F023EE21B341B01 - 150347 - nan
    Skipping row with missing dateid: A447059F0A0A535F - 150348 - nan
    Skipping row with missing dateid: BD1DC6DBB1BEEDA6 - 150349 - nan
    Skipping row with missing dateid: B4E3FBEAC3A4D89B - 150350 - nan
    Skipping row with missing dateid: 00AA8E11C2BFB34D - 150351 - nan
    Skipping row with missing dateid: A9E71838191E720E - 150352 - nan
    Skipping row with missing dateid: 96FBD0718EF090C2 - 150353 - nan
    Skipping row with missing dateid: 35D4D11A427A90E4 - 150354 - nan
    Skipping row with missing dateid: 8280CBC86B8340C7 - 150355 - nan
    Skipping row with missing dateid: 04B576B2099B5407 - 150356 - nan
    Skipping row with missing dateid: F795C79947839F13 - 150357 - nan
    Skipping row with missing dateid: C197E1FCB6C6A31F - 150358 - nan
    Skipping row with missing dateid: 73F9BF8F1A9CC252 - 150359 - nan
    Skipping row with missing dateid: B93018B4CB0BA992 - 150360 - nan
    Skipping row with missing dateid: E35886FD66CCC23D - 150361 - nan
    Skipping row with missing dateid: 5489FEEC73FAD213 - 150362 - nan
    Skipping row with missing dateid: 64855ADA3DCA5237 - 150363 - nan
    Skipping row with missing dateid: EAD8E1E2F98AEE07 - 150364 - nan
    Skipping row with missing dateid: 972D047D34A5A93D - 150365 - nan
    Skipping row with missing dateid: 08C3A66FD22AE2AD - 150366 - nan
    Skipping row with missing dateid: 26F09AED70BAAE91 - 150367 - nan
    Skipping row with missing dateid: F5F41C1A1A19ADB0 - 150368 - nan
    Skipping row with missing dateid: F3BE212BA8CB0250 - 150369 - nan
    Skipping row with missing dateid: 8CC6FCEAD484AD38 - 150370 - nan
    Skipping row with missing dateid: 4937FC9B381BFAD3 - 150371 - nan
    Skipping row with missing dateid: A6523352A8718A6D - 150372 - nan
    Skipping row with missing dateid: 90571C8488518752 - 150373 - nan
    Skipping row with missing dateid: 91D8F6CCD16D94BC - 150374 - nan
    Skipping row with missing dateid: 1E37D735AE5D2327 - 150375 - nan
    Skipping row with missing dateid: D14D72F0E25811E4 - 150376 - nan
    Skipping row with missing dateid: 21CF142E557E12DA - 150377 - nan
    Skipping row with missing dateid: 6411321B651FEB3F - 150378 - nan
    Skipping row with missing dateid: BF85342063BE969A - 150379 - nan
    Skipping row with missing dateid: A055F2374CE790B4 - 150380 - nan
    Skipping row with missing dateid: D1573F51EE099C51 - 150381 - nan
    Skipping row with missing dateid: 8E3AB75E320F9918 - 150382 - nan
    Skipping row with missing dateid: 512A0B058C9C9C9C - 150383 - nan
    Skipping row with missing dateid: FF352378CE26DEB1 - 150384 - nan
    Skipping row with missing dateid: C039BF3CD1C10E71 - 150385 - nan
    Skipping row with missing dateid: 3088E6552D12AD74 - 150386 - nan
    Skipping row with missing dateid: B98B5F1E457BD07E - 150387 - nan
    Skipping row with missing dateid: B1040B1F0C9BF68D - 150388 - nan
    Skipping row with missing dateid: 475E7D449B4BB9A2 - 150389 - nan
    Skipping row with missing dateid: E3E2E87E45A55330 - 150390 - nan
    Skipping row with missing dateid: 786D01D945F7B9FC - 150391 - nan
    Skipping row with missing dateid: EF008404508FCD83 - 150392 - nan
    Skipping row with missing dateid: 3EB65E8151C6BB43 - 150393 - nan
    Skipping row with missing dateid: 3FD581BDA7CF0BFD - 150394 - nan
    Skipping row with missing dateid: 7B713183C2DEF60F - 150395 - nan
    Skipping row with missing dateid: 92D94DFE9768B962 - 150396 - nan
    Skipping row with missing dateid: 609703401360E7A1 - 150397 - nan
    Skipping row with missing dateid: F25D712DDFC2C91C - 150398 - nan
    Skipping row with missing dateid: D623AA8D01603D3D - 150399 - nan
    Skipping row with missing dateid: E1BDF6CE1FF53D11 - 150400 - nan
    Skipping row with missing dateid: 9D7BF4D032963113 - 150401 - nan
    Skipping row with missing dateid: 125D43679A2BB037 - 150402 - nan
    Skipping row with missing dateid: 6FC061EB5DE8CB0A - 150403 - nan
    Skipping row with missing dateid: 0CE1C32FA2053810 - 150404 - nan
    Skipping row with missing dateid: 64EC93B31C69E9CB - 150405 - nan
    Skipping row with missing dateid: E9E6B1324CD0CBAD - 150406 - nan
    Skipping row with missing dateid: 216E0370C45C536D - 150407 - nan
    Skipping row with missing dateid: 9DCEC62F3A432E6C - 150408 - nan
    Skipping row with missing dateid: E04198FB0E09B705 - 150409 - nan
    Skipping row with missing dateid: 043453B16E675C76 - 150410 - nan
    Skipping row with missing dateid: B1B084D9D75D9983 - 150411 - nan
    Skipping row with missing dateid: 26435DEF8FDC1C0E - 150412 - nan
    Skipping row with missing dateid: 54863486FAC70B13 - 150413 - nan
    Skipping row with missing dateid: AFFAD2654C9CB49B - 150414 - nan
    Skipping row with missing dateid: 666E49031A7688DD - 150415 - nan
    Skipping row with missing dateid: 7582229C78B404D0 - 150416 - nan
    Skipping row with missing dateid: 7604801C11E1091C - 150417 - nan
    Skipping row with missing dateid: AB0E09FBC447D2F8 - 150418 - nan
    Skipping row with missing dateid: 2868750E1739AB67 - 150419 - nan
    Skipping row with missing dateid: 5E04AD8260914F66 - 150420 - nan
    Skipping row with missing dateid: EFDF926ADE1FEBA8 - 150421 - nan
    Skipping row with missing dateid: E47D4AC5FA8AD361 - 150422 - nan
    Skipping row with missing dateid: AD902BFCA884F7E5 - 150423 - nan
    Skipping row with missing dateid: 7B3EE3DCB144E25F - 150424 - nan
    Skipping row with missing dateid: 3AE4ED64CA47A00A - 150425 - nan
    Skipping row with missing dateid: 05C39FD782A17707 - 150426 - nan
    Skipping row with missing dateid: 054CEFE196D535F6 - 150427 - nan
    Skipping row with missing dateid: 083B35AF31522A90 - 150428 - nan
    Skipping row with missing dateid: F6EF96E8BA7D5DE1 - 150429 - nan
    Skipping row with missing dateid: 5C025F5CB0E00C0C - 150430 - nan
    Skipping row with missing dateid: DD7E6243EBA5FBF7 - 150431 - nan
    Skipping row with missing dateid: A78B277EE2A7C237 - 150432 - nan
    Skipping row with missing dateid: 7E2AF997037B3F2A - 150433 - nan
    Skipping row with missing dateid: 76ADB4C46C9B6EC9 - 150434 - nan
    Skipping row with missing dateid: A9B1F23C412740A6 - 150435 - nan
    Skipping row with missing dateid: DA4CE6EA79C5FE02 - 150436 - nan
    Skipping row with missing dateid: 32AB30697EBE992C - 150437 - nan
    Skipping row with missing dateid: 1CE941AEF96D2650 - 150438 - nan
    Skipping row with missing dateid: 8AD0D777A2940D02 - 150439 - nan
    Skipping row with missing dateid: 7F0797BC5B7A9A63 - 150440 - nan
    Skipping row with missing dateid: CBED8F43FF498548 - 150441 - nan
    Skipping row with missing dateid: CED63DB8ABB5967F - 150442 - nan
    Skipping row with missing dateid: B8D97E85FFDEA9D7 - 150443 - nan
    Skipping row with missing dateid: 24F0B7C26C3ADD01 - 150444 - nan
    Skipping row with missing dateid: 6F9FD7A2FDB1E43C - 150445 - nan
    Skipping row with missing dateid: B57A2BD92B895732 - 150446 - nan
    Skipping row with missing dateid: EF0545301F159D4C - 150447 - nan
    Skipping row with missing dateid: 18AD88893B820EBD - 150448 - nan
    Skipping row with missing dateid: 5CEFA2B575FC99CD - 150449 - nan
    Skipping row with missing dateid: 6F9103FEE8AA53F6 - 150450 - nan
    Skipping row with missing dateid: A5C77091CD76521F - 150451 - nan
    Skipping row with missing dateid: 5F5E60937CD05BFA - 150452 - nan
    Skipping row with missing dateid: 467FCE19E91CE7C6 - 150453 - nan
    Skipping row with missing dateid: CF22D91834D55B49 - 150454 - nan
    Skipping row with missing dateid: B5381483BC8DD39C - 150455 - nan
    Skipping row with missing dateid: EAF4289981C8C887 - 150456 - nan
    Skipping row with missing dateid: 16BA3EA60B20CC89 - 150457 - nan
    Skipping row with missing dateid: 2538C60A49633323 - 150458 - nan
    Skipping row with missing dateid: 863FC8667219D520 - 150459 - nan
    Skipping row with missing dateid: 812E24618ADE7356 - 150460 - nan
    Skipping row with missing dateid: 14BB5399BAC08CDF - 150461 - nan
    Skipping row with missing dateid: BBF17F8D66E76736 - 150462 - nan
    Skipping row with missing dateid: 0CB28F5C5986D5F1 - 150463 - nan
    Skipping row with missing dateid: C2B043B78E26152A - 150464 - nan
    Skipping row with missing dateid: 4D26D39B6391A5C2 - 150465 - nan
    Skipping row with missing dateid: D7DB84A71ED3556B - 150466 - nan
    Skipping row with missing dateid: A92E80AE753EE6FC - 150467 - nan
    Skipping row with missing dateid: 019A61B569086AAC - 150468 - nan
    Skipping row with missing dateid: 271C8E9BF8DA8CDF - 150469 - nan
    Skipping row with missing dateid: DD8E941D4B97CF55 - 150470 - nan
    Skipping row with missing dateid: 887162E9B3FCA21C - 150471 - nan
    Skipping row with missing dateid: D185BBF63F92F357 - 150472 - nan
    Skipping row with missing dateid: 94FBB5642F296376 - 150473 - nan
    Skipping row with missing dateid: 911C9A2A4846AECA - 150474 - nan
    Skipping row with missing dateid: 3151DC0887089EA1 - 150475 - nan
    Skipping row with missing dateid: 6A8A34C79F19B49C - 150476 - nan
    Skipping row with missing dateid: 4C923804D775B894 - 150477 - nan
    Skipping row with missing dateid: E6829EDDF3DC2EC4 - 150478 - nan
    Skipping row with missing dateid: D43AD1A51BE671EF - 150479 - nan
    Skipping row with missing dateid: 75994ED2BB3D2520 - 150480 - nan
    Skipping row with missing dateid: A37569F96BC96EEE - 150481 - nan
    Skipping row with missing dateid: 54EEB46366D90B68 - 150482 - nan
    Skipping row with missing dateid: 4521389EF3F39692 - 150483 - nan
    Skipping row with missing dateid: 61FFBA02A342CAD9 - 150484 - nan
    Skipping row with missing dateid: F0FD81A9B12E48D7 - 150485 - nan
    Skipping row with missing dateid: 0DB64B6AC13F675D - 150486 - nan
    Skipping row with missing dateid: 8AE5DD9D07A29FB7 - 150487 - nan
    Skipping row with missing dateid: AD4E5DBDC940D209 - 150488 - nan
    Skipping row with missing dateid: CBBAAB993E7EB4D4 - 150489 - nan
    Skipping row with missing dateid: CC647E1A342394DC - 150490 - nan
    Skipping row with missing dateid: 5CAACEA3C89AA1D7 - 150491 - nan
    Skipping row with missing dateid: F8065EB36B72B09D - 150492 - nan
    Skipping row with missing dateid: CD19A42AB0EF3C88 - 150493 - nan
    Skipping row with missing dateid: E7E41AEF4B59986D - 150494 - nan
    Skipping row with missing dateid: 41A9B5F52C6892FD - 150495 - nan
    Skipping row with missing dateid: 0574EC965E0AE9F0 - 150496 - nan
    Skipping row with missing dateid: 5605CE08A00DC88B - 150497 - nan
    Skipping row with missing dateid: 62AFD998EF104E69 - 150498 - nan
    Skipping row with missing dateid: 83FF77F2279E24ED - 150499 - nan
    Skipping row with missing dateid: 77B4599EE7160C46 - 150500 - nan
    Skipping row with missing dateid: 131BEF799318B7F7 - 150501 - nan
    Skipping row with missing dateid: CD8F4141B4D0F4C1 - 150502 - nan
    Skipping row with missing dateid: 3A03417FB8C56751 - 150503 - nan
    Skipping row with missing dateid: 29EAD0EEEC1D3B46 - 150504 - nan
    Skipping row with missing dateid: E012DFA1C3DF0953 - 150505 - nan
    Skipping row with missing dateid: D99FC74E6DD6D0E5 - 150506 - nan
    Skipping row with missing dateid: 6A70021702E888DB - 150507 - nan
    Skipping row with missing dateid: F297FCD0EF2346F7 - 150508 - nan
    Skipping row with missing dateid: D4D3D32B25BC73EA - 150509 - nan
    Skipping row with missing dateid: 748307A4CD478350 - 150510 - nan
    Skipping row with missing dateid: 0A93E1478910800A - 150511 - nan
    Skipping row with missing dateid: 63CD23D63350CE74 - 150512 - nan
    Skipping row with missing dateid: EF08E1B50898E994 - 150513 - nan
    Skipping row with missing dateid: CD6CDE73FD19EDDE - 150514 - nan
    Skipping row with missing dateid: BCC6FCAE60EC809A - 150515 - nan
    Skipping row with missing dateid: F1E96A757E60A5C4 - 150516 - nan
    Skipping row with missing dateid: AA6B83DFB394E606 - 150517 - nan
    Skipping row with missing dateid: C63FEB8F32329102 - 150518 - nan
    Skipping row with missing dateid: E9F099AA3A8A761B - 150519 - nan
    Skipping row with missing dateid: 48D919AE9FF3E963 - 150520 - nan
    Skipping row with missing dateid: A017DC3F246FC29D - 150521 - nan
    Skipping row with missing dateid: 8D08760C95F5209E - 150522 - nan
    Skipping row with missing dateid: 597570C28C9F5476 - 150523 - nan
    Skipping row with missing dateid: 5563E1734BA68E97 - 150524 - nan
    Skipping row with missing dateid: A27EEAF8CD7DC329 - 150525 - nan
    Skipping row with missing dateid: 122BE7A6A567E4D8 - 150526 - nan
    Skipping row with missing dateid: 7A2585B41662D279 - 150527 - nan
    Skipping row with missing dateid: EFEB5CC134568F2F - 150528 - nan
    Skipping row with missing dateid: A5198188F72A5B48 - 150529 - nan
    Skipping row with missing dateid: DD3A9D4A68DD5E80 - 150530 - nan
    Skipping row with missing dateid: 7B0DE25826D05DB9 - 150531 - nan
    Skipping row with missing dateid: C35269694AAE9984 - 150532 - nan
    Skipping row with missing dateid: 0765392A041B8E3C - 150533 - nan
    Skipping row with missing dateid: 4DAA804E2AA0CAAE - 150534 - nan
    Skipping row with missing dateid: 178C139CBB316A90 - 150535 - nan
    Skipping row with missing dateid: 1F43B90E7280D2E4 - 150536 - nan
    Skipping row with missing dateid: 475F8F8E4871BB3A - 150537 - nan
    Skipping row with missing dateid: FB21AC2BE86791EC - 150538 - nan
    Skipping row with missing dateid: A77FEF0948DEB492 - 150539 - nan
    Skipping row with missing dateid: 515B645D2EC1BFC0 - 150540 - nan
    Skipping row with missing dateid: 2DA9295603CA3EEE - 150541 - nan
    Skipping row with missing dateid: 732AFB4B036569CA - 150542 - nan
    Skipping row with missing dateid: 4E04928242CD9FB6 - 150543 - nan
    Skipping row with missing dateid: 54D39FE5E3E84E98 - 150544 - nan
    Skipping row with missing dateid: 0B097E702A748261 - 150545 - nan
    Skipping row with missing dateid: 4E5F46B5EE5F25ED - 150546 - nan
    Skipping row with missing dateid: 1E1E4C564CAFAA09 - 150547 - nan
    Skipping row with missing dateid: F4D6E8311262C9B5 - 150548 - nan
    Skipping row with missing dateid: 6AB070E4FD2C9CB3 - 150549 - nan
    Skipping row with missing dateid: 69201EC94F00A406 - 150550 - nan
    Skipping row with missing dateid: 14E57BA569316DFF - 150551 - nan
    Skipping row with missing dateid: 17B1CC0465629014 - 150552 - nan
    Skipping row with missing dateid: D4499C6123FDAC8B - 150553 - nan
    Skipping row with missing dateid: 4A79245F75C206D6 - 150554 - nan
    Skipping row with missing dateid: FCFEC631B372EA3A - 150555 - nan
    Skipping row with missing dateid: 4EB6931B40FF1962 - 150556 - nan
    Skipping row with missing dateid: E381DC89C578274F - 150557 - nan
    Skipping row with missing dateid: 7019CEF94FFEA1D9 - 150558 - nan
    Skipping row with missing dateid: 7954FE7E20801979 - 150559 - nan
    Skipping row with missing dateid: BF66290E52BE89EF - 150560 - nan
    Skipping row with missing dateid: ADF419E0F7955918 - 150561 - nan
    Skipping row with missing dateid: 89DE4A6A1BDF845B - 150562 - nan
    Skipping row with missing dateid: 2B6A292E24ED66F2 - 150563 - nan
    Skipping row with missing dateid: 47FD1AA3AE799F4A - 150564 - nan
    Skipping row with missing dateid: 699EC8553FA9535E - 150565 - nan
    Skipping row with missing dateid: 903ED4587F565259 - 150566 - nan
    Skipping row with missing dateid: 0EDFE6308C092BCC - 150567 - nan
    Skipping row with missing dateid: 78CEC0AD55D6AA97 - 150568 - nan
    Skipping row with missing dateid: 8A8FA404B8B93846 - 150569 - nan
    Skipping row with missing dateid: 583EED5D63C1A951 - 150570 - nan
    Skipping row with missing dateid: F468E246A403F925 - 150571 - nan
    Skipping row with missing dateid: 65DA5865110D5772 - 150572 - nan
    Skipping row with missing dateid: 19C60B0248310B79 - 150573 - nan
    Skipping row with missing dateid: D109095DEE73E191 - 150574 - nan
    Skipping row with missing dateid: 10015B521BE73A9B - 150575 - nan
    Skipping row with missing dateid: 92220992DF98702E - 150576 - nan
    Skipping row with missing dateid: FF5F59D32EAD8167 - 150577 - nan
    Skipping row with missing dateid: 3E04880F18544836 - 150578 - nan
    Skipping row with missing dateid: B4F679E51E49A915 - 150579 - nan
    Skipping row with missing dateid: C78DB69F78A53A96 - 150580 - nan
    Skipping row with missing dateid: 54DFB8C2BB2E63B9 - 150581 - nan
    Skipping row with missing dateid: E45FBFFF9AFBFD19 - 150582 - nan
    Skipping row with missing dateid: 44CE2C0BBCB6FEBD - 150583 - nan
    Skipping row with missing dateid: 81FB672B066A27A8 - 150584 - nan
    Skipping row with missing dateid: 86108FF80C661DE2 - 150585 - nan
    Error occurred: 'userTypeid'


## Notes:
### hard reset of database


```python
session.rollback()
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text("DROP SCHEMA public CASCADE;"))
    conn.execute(text("CREATE SCHEMA public;"))
    conn.commit()

Base.metadata.create_all(engine)


```

### util methods for debugging


```python
print(df_all.columns.tolist())
print(df_all.head())

```

    ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual', 'duration', 'hour', 'day', 'month', 'year', 'minute', 'userTypeId', 'usertypeid', 'startdateid', 'stopdateid']
                ride_id  rideable_type              started_at  \
    0  28A17ACD224CD80B  electric_bike 2024-12-06 17:50:49.428   
    1  3508393A86FBD357   classic_bike 2024-12-14 11:01:00.309   
    2  75FA4C03A1447401  electric_bike 2024-12-24 08:07:17.475   
    3  C7741EF495C597DD   classic_bike 2024-12-19 12:48:05.452   
    4  07952BB20B46C5B1  electric_bike 2024-12-17 11:19:37.631   
    
                     ended_at start_station_name start_station_id  \
    0 2024-12-06 17:54:20.070        Oakland Ave            JC022   
    1 2024-12-14 11:12:01.382        Oakland Ave            JC022   
    2 2024-12-24 08:14:14.612        Oakland Ave            JC022   
    3 2024-12-19 12:54:15.253        Oakland Ave            JC022   
    4 2024-12-17 11:28:25.150        Oakland Ave            JC022   
    
                               end_station_name end_station_id  start_lat  \
    0                                   Hilltop          JC019  40.737604   
    1  Hoboken Terminal - Hudson St & Hudson Pl          HB101  40.737604   
    2                       Leonard Gordon Park          JC080  40.737604   
    3                       Leonard Gordon Park          JC080  40.737604   
    4                             Grove St PATH          JC115  40.737604   
    
       start_lng  ...               duration  hour day month  year  minute  \
    0 -74.052478  ... 0 days 00:03:30.642000    17   6    12  2024      50   
    1 -74.052478  ... 0 days 00:11:01.073000    11  14    12  2024       1   
    2 -74.052478  ... 0 days 00:06:57.137000     8  24    12  2024       7   
    3 -74.052478  ... 0 days 00:06:09.801000    12  19    12  2024      48   
    4 -74.052478  ... 0 days 00:08:47.519000    11  17    12  2024      19   
    
       userTypeId  usertypeid  startdateid  stopdateid  
    0           1         NaN            1         NaN  
    1           1         NaN            2         NaN  
    2           1         NaN            3         NaN  
    3           1         NaN            4         NaN  
    4           2         NaN            5         NaN  
    
    [5 rows x 23 columns]

