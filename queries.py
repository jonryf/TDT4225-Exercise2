from DbConnector import DbConnector
from haversine import haversine

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def q1(self):
        tables = ["Activity", "TrackPoint", "User"]
        for table in tables:
            query = "SELECT COUNT(id) FROM {}".format(table)
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            nr = rows[0][0]
            print("There are {} entries in the {} table".format(nr, table))

    def q4(self):
        query = "SELECT DISTINCT user_id FROM Activity " \
                "WHERE transportation_mode='taxi'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        rows = [entry[0] for entry in rows]
        print("The following users have taken a taxi:", rows)

    def q7(self):
        query = "SELECT id FROM Activity " \
                "WHERE user_id='112' AND transportation_mode='walk' AND " \
                "(YEAR(start_date_time)=2008 OR YEAR(end_date_time)=2008)"
        self.cursor.execute(query)
        activities = [result[0] for result in self.cursor.fetchall()]

        total_distance = 0
        for activity in activities:
            query = "SELECT lat,lon FROM TrackPoint WHERE activity_id={} AND YEAR(date_time)=2008 ORDER BY date_time".format(activity)
            self.cursor.execute(query)
            coordinates = self.cursor.fetchall()
            for i in range(len(coordinates)-1):
                coord1 = coordinates[i]
                coord2 = coordinates[i+1]
                dist = haversine(coord1, coord2)
                total_distance += dist

        print("user 112 walked a total of {} km in 2008".format(round(total_distance,2)))

    def q10(self):
        # coordinates of the forbidden city. Round down to two decimals to get matches.
        lat = round(39.916, 2)
        lon = round(116.397, 2)

        query = "SELECT DISTINCT user_id from Activity " \
                "JOIN TrackPoint ON Activity.id=TrackPoint.activity_id " \
                "WHERE ROUND(lat,2)={} AND ROUND(lon,2)={}".format(lat, lon)
        self.cursor.execute(query)
        users = [entry[0] for entry in self.cursor.fetchall()]
        print("The following users have tracked an activity in the Forbidden City:", users)
