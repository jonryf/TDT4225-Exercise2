from DbConnector import DbConnector
from haversine import haversine
from tabulate import tabulate

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

    def q2(self):
        query_activities = "SELECT COUNT(id) FROM Activity"
        query_users = "SELECT COUNT(id) FROM User"

        self.cursor.execute(query_activities)
        count_activities = self.cursor.fetchall()

        self.cursor.execute(query_users)
        count_users = self.cursor.fetchall()

        avg = count_activities[0][0] / count_users[0][0]
        print("The average number of activites per user is: {}".format(round(avg, 0), float))


    def q4(self):
        query = "SELECT DISTINCT user_id FROM Activity " \
                "WHERE transportation_mode='taxi'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("The following users have taken a taxi:")
        print(tabulate(rows, headers=self.cursor.column_names))

    def q5(self):
        query =  "SELECT COUNT(id), transportation_mode FROM Activity GROUP BY transportation_mode"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

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

    def q8(self):
        query = "SELECT Activity.user_id, SUM(TP1.altitude - (" \
                "SELECT altitude FROM TrackPoint TP2 " \
                "WHERE TP2.id = TP1.id - 1 AND TP2.altitude < TP1.altitude AND TP2.activity_id = TP1.activity_id " \
                "ORDER BY id LIMIT 1)) altitude " \
                "FROM TrackPoint TP1 JOIN Activity ON TP1.activity_id = Activity.id WHERE TP1.altitude != -777 AND altitude > 0 GROUP BY Activity.user_id ORDER BY altitude DESC LIMIT 20"

        self.cursor.execute(query)
        altitudes = self.cursor.fetchall()

        for k in range(len(altitudes)):
            if altitudes[k][1] is None:
                continue
            altitudes[k] = (altitudes[k][0], float(altitudes[k][1]) * 0.3048)

        print("Top 20 users with meters gained in total altitude")
        print(tabulate(altitudes, headers=self.cursor.column_names))

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

    def q11(self):
        #FIKS DETTE!!
        query = "SELECT DISTINCT user_id, transportation_mode, COUNT(transportation_mode) " \
                "FROM Activity GROUP BY user_id, transportation_mode ORDER BY user_id"
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print(tabulate(result, headers=self.cursor.column_names))

def main():
    program = None
    try:
        program = Queries()
        print("QUESTIONS:")
        print("\nQuestion 1: ")
        program.q1()
        print("\nQuestion 2: ")
        program.q2()
        print("\nQuestion 3: ")
        print("\nQuestion 4: ")
        program.q4()
        print("\nQuestion 5: ")
        program.q5()
        print("\nQuestion 6: ")
        print("\nQuestion 7: ")
        #program.q7()
        print("\nQuestion 8: ")
        #program.q8()
        print("\nQuestion 9: ")
        print("\nQuestion 10: ")
        program.q10()
        print("\nQuestion 11: ")
        program.q11()
    except Exception as e:
        print("ERROR: failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
