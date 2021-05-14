import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from Create_Table_queries import time_table_insert, \
    users_table_insert, youtubers_table_insert, \
    videos_table_insert, video_play_table_insert
import time
from datetime import datetime
from random import randint
import uuid


def process_youtubedata_file(cur, conn, filepath):
    """
        This function reads one JSON file and read information of videos
        and youtuber data and saves into video_data and youtuber_data.
        Arguments:
        cur: Database Cursor
        filepath: location of JSON files
        Return: None
    """
    # open JSON file
    df = pd.read_json(filepath)

    # ---------insert youtube record----------
    # reads you tube data from JSON file and insert it into Youtubers_dim table
    youtuber_id = df.youtuber_id._values[0]
    youtuber_name = df.youtuber_name._values[0]
    youtuber_location = df.youtuber_location._values[0]
    youtuber_latitude = df.youtuber_latitude._values[0]
    youtuber_longitude = df.youtuber_longitude._values[0]

    check_duplicate_yt = "SELECT youtuber_id from youtubers_dim WHERE youtuber_id = %s"
    cur.execute(check_duplicate_yt, (youtuber_id,))
    row = cur.fetchone()

    if row:
        print('Already present ID: Youtube', youtuber_id)
    else:
        yt_data = (youtuber_id, youtuber_name, youtuber_location, youtuber_latitude, youtuber_longitude)
        cur.execute(youtubers_table_insert, yt_data)

    # ---------insert video record--------------
    # reads youtube videos data from JSON file and insert it into Videos_dim table
    video_id = df.video_id._values[0]
    title = df.title._values[0]
    youtuber_id = df.youtuber_id._values[0]
    year = int(str(df.year._values[0]))
    duration = df.duration._values[0]

    check_duplicate_vd = "SELECT video_id from videos_dim WHERE video_id = %s"
    cur.execute(check_duplicate_vd, (video_id,))
    row = cur.fetchone()

    if row:
        print('Already present ID: Video', video_id)
    else:
        v_data = (video_id, title, youtuber_id, year, duration)
        cur.execute(videos_table_insert, v_data)


def process_time_stamp(timestamp):
    """
        This function process Unix time stamp as a input argument.
        Arguments:
        timestamp: Unix Time stamp
        Return: List of time stamp objects.
    """
    timestamp /= 1000  # Handle year bound, out of range error.
    _process_ts = datetime.utcfromtimestamp(timestamp).strftime('%d %m %Y %W %w %H:%M:%S')
    _process_ts = _process_ts.split(' ')  # split timestamp string into list.
    return _process_ts


def process_log_file(cur, conn, filepath):
    """
        This function reads Log files and reads information of time, user and videoplay data and saves into time, user, videoplay
        Arguments:
        cur: Database Cursor
        filepath: location of Log files
        Return: None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextVideo action
    df[(df['page'] == 'NextVideo')]

    for index, row in df.iterrows():

        # convert timestamp column to datetime
        _ts_process = process_time_stamp(row['ts'])
        hour = _ts_process[5].split(':')[0]
        day = _ts_process[0]
        week = _ts_process[3]
        month = _ts_process[1]
        year = _ts_process[2]
        weekday = _ts_process[4]
        start_time = year + '-' + month + '-' + day

        check_duplicate_time = "SELECT start_time from time_dim WHERE start_time = %s"
        cur.execute(check_duplicate_time, (start_time,))
        _duplicate_time = cur.fetchone()

        if _duplicate_time:
            print('Already present Start time:', _duplicate_time)
        else:
            time_data = (str(start_time), hour, day, week, month, year, weekday)
            cur.execute(time_table_insert, time_data)

        # load user table
        user_id = row['userId']
        first_name = row['firstName']
        last_name = row['lastName']
        gender = row['gender']
        level = row['level']

        if user_id is not None:
            check_duplicate_user = "SELECT user_id from users_dim WHERE user_id = %s"
            cur.execute(check_duplicate_user, (str(user_id),))
            _duplicate_user = cur.fetchone()

            if _duplicate_user:
                print('Already present ID: User', check_duplicate_user)
            else:
                user_data = (user_id, first_name, last_name, gender, level)
                cur.execute(users_table_insert, user_data)

        if row['youtuber'] is not None:
            get_youtuber_id = "SELECT youtuber_id from youtubers_dim WHERE name = %s"
            cur.execute(get_youtuber_id, (row['youtuber'],))
            yt_row = cur.fetchone()
            if yt_row is not None:
                _youtuber_id = yt_row[0]
                get_videos_id = "SELECT video_id from videos_dim WHERE youtuber_id = %s"
                cur.execute(get_videos_id, (_youtuber_id,))
                vd_row = cur.fetchone()
                if vd_row is not None:
                    _vd_id = vd_row[0]
                else:
                    _vd_id = None
            else:
                _youtuber_id = None
                _vd_id = None
        else:
            _youtuber_id = None
            _vd_id = None

        # insert Videoplay records in Videoplay_fact table
        uu_id = uuid.uuid4().hex
        video_play_fact_data = (uu_id, start_time, user_id, level,
                                _vd_id, _youtuber_id, row['sessionId'],
                                row['location'], row['userAgent'])
        cur.execute(video_play_table_insert, video_play_fact_data)
        conn.commit()


def process_data(cur, conn, filepath, func):
    """
        This function get all JSON files in given directory by exploring all sub directories, and process all files that were found using the given function.
        Example: if I give it the path to youtube_data directory which resides in data folder of this assignment,
        and func given is process_youtubedata_file it should get all JSON files in this directories and process each file using process_youtubedata_file function.
        Arguments:
        cur: Database Cursor
        conn: Database
        filepath: location of JSON files
        func: function to process all files in the directory
        Return: None
    """
    for root, dirs, files in os.walk(filepath):
        for name in files:
            if name.endswith((".json")):
                full_path = os.path.join(root, name)
                func(cur, conn, full_path)
                conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=youtubedb user=postgres password=admin")
    cur = conn.cursor()
    # process_data(cur, conn, filepath='data/youtube_data', func=process_youtubedata_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()