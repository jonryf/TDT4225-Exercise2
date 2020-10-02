from DbConnector import DbConnector

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

