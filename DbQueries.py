from DbConnector import DbConnector
from tabulate import tabulate

class DbQueries:
    def __init__(self):
        '''
        Connect to database
        '''
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_avg_activities_per_user(self):
        query_activity = "SELECT COUNT(id) FROM Activity"
        query_user = "SELECT COUNT(id) FROM User"


        self.cursor.execute(query_activity)
        avg_activities = self.cursor.fetchall()

        self.cursor.execute(query_user)
        numb_users = self.cursor.fetchall()

        print(float(avg_activities[0][0] / numb_users[0][0]))

        self.db_connection.commit()
        print("Data from table Activity, raw format: ")
        print(avg_activities)

        # Using tabulate to show the table in a nice way
        print("Data from table Activity, tabulated:")
        print(tabulate(avg_activities, headers=self.cursor.column_names))

    def fetch_count_transportation_lables(self):
        query = "SELECT"

def main():
    program = None
    try:
        program = DbQueries()
        program.fetch_avg_activities_per_user()
    except Exception as e:
        print("ERROR: failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
