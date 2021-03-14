import sqlite3
import pandas as pd


class DBConnection:

    def __init__(self, db='airbnb_listings2.db'):
        self.name = db
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()

class ReadCSV(DBConnection):

    def __init__(self, csv='Singapore_Airbnb_Data.csv'):
        super().__init__()
        self.csv = csv
        self.listings = pd.read_csv(csv)
        self.listings.to_sql = self.listings.to_sql('airbnb_listings2', self.conn, if_exists='append', index=False)
        print("Successfully read from CSV")


class Commit(DBConnection):

    def commit(self):
        self.conn.commit()
        print("Successfully Committed")
        return


class CreateTables(DBConnection):

    def __init__(self):
        super().__init__()

    def DropTables(self, table):
        sql_clause = f'DROP TABLE IF EXISTS {table};'
        self.cursor.execute(sql_clause)
        print("Successfully Dropped Table called", table, )

    def CreateHosts(self):
        sql_clause = f'CREATE TABLE IF NOT EXISTS hosts (host_id INTEGER NOT NULL PRIMARY KEY, ' \
                     f'host_name TEXT NOT NULL, calculated_host_listings_count INTEGER NOT NULL);'
        self.cursor.execute(sql_clause)
        print("Hosts Table Created in AirBnb Database")

    def CreateLocations(self):
        sql_clause = f'CREATE TABLE IF NOT EXISTS locations (location_id INTEGER NOT NULL PRIMARY KEY,' \
                     f'neighbourhood TEXT,neighbourhood_group TEXT);'
        self.cursor.execute(sql_clause)
        print("Locations Table Created in AirBnb Database")

    def CreateListings(self):
        sql_clause = f'CREATE TABLE IF NOT EXISTS airbnb (id INTEGER PRIMARY KEY, host_id INTEGER,' \
                     f'location_id INTEGER, name TEXT, latitude TEXT, longitude TEXT, room_type TEXT,' \
                     f'price INTEGER, minimum_nights INTEGER, number_of_reviews INTEGER, last_review INTEGER,' \
                     f'reviews_per_month INTEGER, availability_365 INTEGER);'
        self.cursor.execute(sql_clause)
        print("Listings Table Created in AirBnb Database")


class InsertInto(Commit):

    def __init__(self):
        super().__init__()

    def InsertHosts(self):
        sql_clause = f'INSERT OR REPLACE INTO hosts(host_id, host_name, calculated_host_listings_count) ' \
                     f'SELECT DISTINCT ' \
                     f'host_id, host_name, calculated_host_listings_count FROM airbnb_listings2'
        self.cursor.execute(sql_clause)
        print("Successfully imported Host values from CSV")

    def InsertLocations(self):
        sql_clause = f'INSERT OR REPLACE INTO locations (neighbourhood, neighbourhood_group)' \
                     f'SELECT DISTINCT neighbourhood, neighbourhood_group FROM airbnb_listings2'
        self.cursor.execute(sql_clause)
        print("Successfully imported Location values from CSV")

    def InsertListings(self):
        sql_clause = f'INSERT OR REPLACE INTO airbnb (id, host_id, name, location_id, latitude, longitude, ' \
                     f'room_type, price, minimum_nights, number_of_reviews, last_review, ' \
                     f'reviews_per_month, availability_365)' \
                     f'SELECT ' \
                     f'id, host_id, name, loc.location_id, latitude, longitude, room_type, ' \
                     f'price, minimum_nights, number_of_reviews, last_review, reviews_per_month, ' \
                     f'availability_365 ' \
                     f'FROM airbnb_listings2 li ' \
                     f'INNER JOIN ' \
                     f'locations loc on li.neighbourhood_group = ' \
                     f'loc.neighbourhood_group and li.neighbourhood = loc.neighbourhood'
        self.cursor.execute(sql_clause)
        print("Successfully imported Listing values from CSV")


connect = Commit()
read = ReadCSV()
create = CreateTables()
create.DropTables('hosts')
create.DropTables('airbnb')
create.DropTables('locations')
create.CreateHosts()
create.CreateListings()
create.CreateLocations()


insert = InsertInto()
insert.InsertHosts()
insert.InsertLocations()
insert.InsertListings()
insert.commit()
