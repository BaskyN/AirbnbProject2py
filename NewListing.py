import sqlite3

# This Program allows a user to enter new details about a property following a series of prompts.
# The databases will update after completion.
# Check FindListing.py to check new entry has been added to the tables.
# The NewListing class inherits a connection to the database from the DBConnect class

from BuildDB import Commit


class NewListing(Commit):

    def __init__(self):
        super().__init__()
        print("FOLLOW INSTRUCTIONS TO ENTER NEW DETAILS, lets start with the host details")

        self.host_name = input("Enter Host Name:")
        while True:
            try:
                self.calculated_host_listings_count = int(input("Enter Listing Count on Airbnb as a number:"))
                break
            except ValueError:
                print("Please input number only")
            continue

    def PrintHost(self):
        print("Name: ", self.host_name)
        print("Current Listing Count: ", self.calculated_host_listings_count)
        print("You have successfully added your host details, now complete the listing details")

    def AddListing(self):
        self.name = input("Listing name: ")

        while True:
            try:
                self.latitude = float(input("Latitude of property:"))
                break
            except ValueError:
                print("Please input number only (for example: 1.42032):")
            continue
        while True:
            try:
                self.longitude = float(input("Longitude of property: "))
                break
            except ValueError:
                print("Please input number only (for example 103.71969): ")
            continue
        self.room_type = input("Room Type (shared room, private room or entire home/apt?): ")
        self.price = int(input("Price per night (number only, ie 100): "))
        self.minimum_nights = int(input("Min nights: "))
        self.number_of_reviews = int(input("Number of Reviews: "))
        self.last_review = input("Last review date (00/00/00): ")
        self.reviews_per_month = float(input("average reviews per month:"))
        self.availability_365 = int(input("Days available throughout the year:"))

    def PrintListing(self):
        print("Listing name: ", self.name)
        print("Latitude: ", self.latitude)
        print("Room Type: ", self.room_type)
        print("Price: ", self.price)
        print("Minimum Nights: ", self.minimum_nights)
        print("Number of Reviews: ", self.number_of_reviews)
        print("Last Review: ", self.last_review)
        print("Reviews per Month: ", self.reviews_per_month)
        print("Annual Availability: ", self.availability_365)
        print("You have successfully input your host details, please complete location details")

    def AddLocation(self):
        print("i.e Central Region, Singapore River, please be mindful of correct spelling")

        self.neighbourhood_group = input("Neighbourhood group: ")
        self.neighbourhood = input("Neighbourhood: ")

    def PrintLocation(self):
        print("Neighbourhood group: ", self.neighbourhood_group)
        print("Neighbourhood", self.neighbourhood)

    def StoreHostDetails(self):
        sql_clause = "INSERT INTO hosts (host_name, calculated_host_listings_count) VALUES (?, ?)"
        values = (self.host_name, self.calculated_host_listings_count)
        self.cursor.execute(sql_clause, values)
        self.conn.commit()
        print('Host details saved to database')
        return self.cursor.lastrowid

    def StoreListingDetails(self):
        sql = """INSERT INTO airbnb (host_id, name, latitude, longitude,
        room_type, price, minimum_nights, number_of_reviews, last_review, 
        reviews_per_month, availability_365) VALUES 
        (?,?,?,?,?,?,?,?,?,?,?)"""
        values = (self.name, self.latitude, self.longitude, self.room_type,
                  self.price, self.minimum_nights, self.number_of_reviews,
                  self.last_review, self.reviews_per_month,
                  self.availability_365)
        self.cursor.execute(sql, (self.cursor.lastrowid,) + values)
        self.conn.commit()
        print('Listing details saved to database')

    def StoreLocationsID(self):
        sql_clause = f"INSERT INTO airbnb (location_id) SELECT location_id FROM locations " \
                     f"WHERE neighbourhood = ? AND neighbourhood_group = ?"
        values = (self.neighbourhood_group, self.neighbourhood)
        self.cursor.execute(sql_clause, values)
        self.conn.commit()
        print('Host details saved to database')


new = NewListing()
new.PrintHost()
new.AddListing()
new.PrintListing()
new.AddLocation()
new.PrintLocation()
new.StoreHostDetails()
new.StoreListingDetails()
new.StoreLocationsID()


