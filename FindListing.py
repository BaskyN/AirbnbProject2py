import sqlite3
import pandas as pd

from BuildDB import DBConnection


# Find listing in the database by a certain parameter
# Delete listing from the database by username

class FindListing(DBConnection):

    def __init__(self):
        pass

    def ByHostID_DF(self, table, value):
        sql_clause = f'SELECT * FROM {table} WHERE host_id = {value} '
        df_table = pd.read_sql_query(sql_clause, self.conn)
        return df_table

    def ByListingID_Print(self, table, value):
        sql_clause = f'SELECT * FROM {table} WHERE id = {value} '
        self.cursor.execute(sql_clause, )
        return self.cursor.fetchall()

    def FindAllByID(self, value):
        sql_clause = f'SELECT airbnb.id, airbnb.name, hos.host_id, hos.host_name, locations.neighbourhood_group, locations.neighbourhood, ' \
                     f'airbnb.latitude, airbnb.longitude, airbnb.room_type, airbnb.price,' \
                     f'airbnb.minimum_nights, airbnb.number_of_reviews, airbnb.last_review, ' \
                     f'airbnb.reviews_per_month, airbnb.availability_365  ' \
                     f'FROM airbnb ' \
                     f'INNER JOIN hosts hos on airbnb.host_id = hos.host_id ' \
                     f'INNER JOIN locations on airbnb.location_id = locations.location_id ' \
                     f'WHERE hos.host_id = {value}'
        self.cursor.execute(sql_clause, )
        return self.cursor.fetchall()

    # How do I print usefully?
    # Wht is the FindALLById not working to find new entries

    def DeleteListing(self, table, host_id):
        sql = f'DELETE FROM {table} WHERE host_id=?'
        self.cursor.execute(sql, (host_id,))
        print(host_id, "Record Deleted")
        return


find = FindListing()

find.ConnectTo()

print(find.ByHostID_DF('airbnb', '456758'))

print(find.ByHostID_DF('hosts', '214040123'))
print(find.FindAllByID('214040123'))  # why is this not working for new entries.
find.DeleteListing('hosts', '456758')

find.commit()
