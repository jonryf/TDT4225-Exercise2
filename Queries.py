from DbConnector import DbConnector
from haversine import haversine
from tabulate import tabulate
import datetime

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

    def q3(self):
        """Find the top 20 users with the highest number of activities"""
        query = "SELECT user_id, COUNT(*) AS Activities FROM Activity GROUP BY user_id ORDER BY Activities DESC LIMIT 20"
        self.cursor.execute(query)
        user_ids = self.cursor.fetchall()
        print("The following users have the highest number of activities")
        print(tabulate(user_ids, headers=self.cursor.column_names))


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

    def q6(self):
        """Find the year with the most activities"""
        years_activities = {}
        years_hours = {}

        for year in range(2007, 2011):
            query_a = "SELECT COUNT(*) FROM Activity WHERE start_date_time LIKE '{}%'".format(year)
            self.cursor.execute(query_a)
            number_of_activities = self.cursor.fetchall()[0]
            years_activities[number_of_activities] = year

            query_b = "SELECT SUM(TIMESTAMPDIFF(HOUR, end_date_time, start_date_time)) FROM Activity WHERE start_date_time LIKE '{}%'".format(year)
            self.cursor.execute(query_b)
            number_of_activities = self.cursor.fetchall()[0]
            years_hours[number_of_activities] = year

        highest_no_of_activities = max(years_activities.keys())
        year = years_activities[highest_no_of_activities]
        print("The year with the most activities is " + str(year))

        most_hours = max(years_hours.keys())
        year = years_hours[most_hours]
        print("The year with the most hours of activities is " + str(year))


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

        for i in range(len(altitudes)):
            if altitudes[i][1] is None:
                continue
            altitudes[i] = (altitudes[i][0], float(altitudes[i][1]) * 0.3048)

        print("Top 20 users with meters gained in total altitude")
        print(tabulate(altitudes, headers=self.cursor.column_names))

    def q9(self):
        """Find all users who have invalid activities, and the number of invalid activities per user"""

        time = "SET @timestamp='0000-00-00 00:00:00'"
        activity_id = "SET @activity_id = 0"
        self.cursor.execute(time)
        self.cursor.execute(activity_id)

        query = "SELECT tabell.user_id AS User, COUNT(DISTINCT tabell.current_id) AS 'Number of invalid Activities' " \
                "FROM (SELECT user_id, @timestamp previous_timestamp, @timestamp:=date_time curr_timestamp, @activity_id previous_id, @activity_id:=Activity.id current_id " \
                "FROM Activity JOIN TrackPoint ON (Activity.id = TrackPoint.activity_id)) AS tabell " \
                "WHERE (TIMESTAMPDIFF(MINUTE, tabell.previous_timestamp, tabell.curr_timestamp) > 5 AND (tabell.current_id = tabell.previous_id))" \
                "GROUP BY User"


        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=self.cursor.column_names))

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
        query = "SELECT user_id, transportation_mode as most_used_transportation_mode, MAX(count1) as count " \
                "FROM (SELECT user_id, transportation_mode, COUNT(*) AS count1 FROM Activity GROUP BY user_id, transportation_mode) as a1 " \
                "GROUP BY user_id, transportation_mode " \
                "ORDER BY user_id, count DESC" \

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        filtered_results = []
        user_id = 0
        for i in range(len(results)):
            # when duplicates, takes the one that is ranked on top my the sql query
            if user_id == results[i][0]:
                continue

            else:
                user_id = results[i][0]
                filtered_results.append((results[i][0], results[i][1]))

        print(tabulate(filtered_results, headers=self.cursor.column_names))

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
        program.q3()
        print("\nQuestion 4: ")
        program.q4()
        print("\nQuestion 5: ")
        program.q5()
        print("\nQuestion 6: ")
        program.q6()
        print("\nQuestion 7: ")
        program.q7()
        print("\nQuestion 8: ")
        program.q8()
        print("\nQuestion 9: ")
        program.q9()
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
