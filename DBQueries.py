import sqlite3
import pandas as pd
from BuildDB import DBConnection


class DBQueries(DBConnection):

    def __init__(self):
        super().__init__()

    def get_tables_name(self):
        sql_clause = f"SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(sql_clause)
        return self.cursor.fetchall()

    def get_df_table(self, table):
        sql_clause = f"SELECT * FROM {table}"
        df_table = pd.read_sql_query(sql_clause, self.conn)
        return df_table

    def insert(self, table, cols, values):
        sql_clause = f"INSERT INTO {table} {cols} VALUES {values}"
        self.cursor.execute(sql_clause)
        return

    def delete_all(self, table):
        sql_clause = f"DELETE FROM {table}"
        self.cursor.execute(sql_clause)
        print("Record Successfully Deleted")
        return

    def delete_byId(self, table, id):
        sql_clause = f'DELETE FROM {table} WHERE id=?'
        self.cursor.execute(sql_clause, (id,))
        return

    def commit(self):
        self.conn.commit()
        return


class CleanData(DBConnection):

    def remove_nulls(self, table, column):
        sql_clause = f'UPDATE {table} SET {column} =0 WHERE {column} is NULL'
        self.cursor.execute(sql_clause, )
        return


c = CleanData()
c.remove_nulls('airbnb', 'last_review')
c.remove_nulls('airbnb', 'reviews_per_month')

