import traceback

from DbConnector import DbConnector
from datetime import datetime

import os

DATASET_ROOT_PATH = "./dataset"
DATASET_PATH = DATASET_ROOT_PATH + "/Data"
DATASET_LABELED_IDS = DATASET_ROOT_PATH + "/labeled_ids.txt"


class DataUploader:

    def __init__(self):
        """
        Upload the dataset to the MySQL database
        """
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

        self.ACTIVITY_ID = 1
        self.TRACKPOINT_ID = 1

    def pack(self, data):
        return [self.pack_data(value) for value in data]

    def pack_data(self, data):
        """
        Add apostrophes to string/date data

        @param data: value
        @return: value with added apostrophes
        """
        if type(data) is bool or str(data).isnumeric():
            return data
        return str(data)

    def insert_data_bulk(self, table_name, data):
        """
        Bulk upload generic data to a table

        @param table_name: table name
        @param data: list of data maps
        """
        if len(data) == 0:
            return

        fields = ", ".join(data[0].keys())
        value_placeholders = ", ".join(["%s" for f in data[0].keys()])
        query = "INSERT INTO %s(%s) VALUES (%s)" % (table_name, fields, value_placeholders)

        data = [tuple(self.pack(data_point.values())) for data_point in data]

        chunk_size = 50000
        data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        for chunk in data_chunks:
            self.cursor.executemany(query, chunk)

    def insert_data(self, table_name, data):
        """
        Upload some generic data to a table

        @param table_name: table name
        @param data: list of data maps
        """
        for data_point in data:
            query = "INSERT INTO %s(%s) VALUES (%s)"

            fields = ", ".join(data_point.keys())
            values = ", ".join([self.pack_data(value) for value in data_point.values()])
            self.cursor.execute(query % (table_name, fields, values))
        self.db_connection.commit()

    def get_labeled_ids(self):
        """
        Read the file with users that has labeled data

        @return: users with labeled ids
        """
        with open(DATASET_LABELED_IDS) as file:
            ids = file.readlines()
            ids = [id.strip() for id in ids]
            return ids

    def read_datetime(self, date_text):
        """
        Convert date string to python date object

        @param date_text: date string
        @return: date python object
        """
        date_text = date_text.replace('/', '-')
        return datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')

    def get_activities(self, user_id, label_file):
        """

        @param user_id:
        @param label_file:
        @return:
        """
        activities = []
        with open(label_file) as file:
            records = file.readlines()[1:]
            for record in records:
                items = [item.strip() for item in record.split("\t")]
                activities.append({
                    "id": self.ACTIVITY_ID,
                    "user_id": user_id,
                    "transportation_mode": items[2],
                    "start_date_time": self.read_datetime(items[0]),
                    "end_date_time": self.read_datetime(items[1]),
                })
                self.ACTIVITY_ID += 1

        return activities

    def get_trackpoints(self, file_path, activities):
        """
        Each trackpoint file contains multiple activities, but they
        are loosely connected to the labeled activity file.

        This method collects the trackpoints for a given trackpoint file and
        maps them to the corresponding activity, based on the timestamps



        @param file_path: trackpoint file
        @param activities: labeled activites for the user
        @return: trackpoints with activity relation
        """
        trackpoints = []
        activity_pointer = 0
        current_activity = activities[activity_pointer]
        collecting_trackpoints = False

        with open(file_path) as file:
            records = file.readlines()[6:]
            if len(records) > 2500:
                return trackpoints
            for record in records:
                items = record.strip().split(",")
                time = self.read_datetime((items[5] + " " + items[6]))

                # find the matching activity for the given trackpoint
                # use the fact that both activities and trackpoints are sorted based on time
                if current_activity["start_date_time"] < time and collecting_trackpoints is False:
                    while current_activity["start_date_time"] < time \
                            and collecting_trackpoints is False and activity_pointer < len(activities):
                        activity_pointer += 1
                        if activity_pointer == len(activities):
                            break
                        current_activity = activities[activity_pointer]

                if current_activity["start_date_time"] == time:
                    collecting_trackpoints = True
                elif current_activity["end_date_time"] == time:
                    collecting_trackpoints = False
                    activity_pointer += 1
                    if activity_pointer >= len(activities):
                        break
                elif not collecting_trackpoints:
                    continue

                trackpoints.append({
                    "activity_id": current_activity["id"],
                    "lat": items[0],
                    "lon": items[1],
                    "altitude": items[3],
                    "date_days": items[4],
                    "date_time": time
                })
                current_activity = activities[activity_pointer]
        return trackpoints

    def upload_data(self):
        """
        Loop over the dataset, and upload it to the database
        """
        labeled_ids = self.get_labeled_ids()

        users = []
        users_ids = []

        activities = []
        last_activities = []

        trackpoints = []

        for root, dirs, files in os.walk(DATASET_PATH, topdown=True):
            path_parts = root.split("/")
            if len(path_parts) < 4:  # check if inside user folder
                continue
            user_id = path_parts[3]

            if user_id not in labeled_ids:
                continue

            if user_id not in users_ids:
                users_ids.append(user_id)
                users.append({"id": user_id, "has_labels": user_id in labeled_ids})

            if 'labels.txt' in files:
                last_activities = self.get_activities(user_id, root + "/labels.txt")
                activities.extend(last_activities)

            if 'Trajectory' in root:
                files.sort()
                for file_path in files:
                    trackpoints.extend(self.get_trackpoints(root + "/" + file_path, last_activities))
            print(len(trackpoints))

        print("Uploading data")
        self.insert_data_bulk("User", users)
        print(" > Users done")
        self.insert_data_bulk("Activity", activities)
        print(" > Activities done")
        self.insert_data_bulk("TrackPoint", trackpoints)
        print(" > TrackPoints done")
        self.cursor.close()

    def clear_db(self):
        """
        Remove all the current data in the database before re-uploading
        """
        self.cursor.execute("DELETE FROM TrackPoint")
        self.cursor.execute("DELETE FROM Activity")
        self.cursor.execute("DELETE FROM User")
        self.db_connection.commit()


def main():
    """
    Clear the tables, and uploads the dataset
    """
    program = None
    try:
        program = DataUploader()
        program.clear_db()
        program.upload_data()
    except Exception as e:
        traceback.print_exc()
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
