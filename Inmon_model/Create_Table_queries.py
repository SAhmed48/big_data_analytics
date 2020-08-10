# DROP TABLES
# Write queries to drop each table, please don't change variable names,
# You should write respective queries against each varibale


video_play_table_drop = "DROP TABLE IF EXISTS videoplay_fact"
users_table_drop = "DROP TABLE IF EXISTS users_dim"
videos_table_drop = "DROP TABLE IF EXISTS videos_dim"
you_tubers_table_drop = "DROP TABLE IF EXISTS youtubers_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# CREATE TABLES
# Write queries to create each table, please don't change variable names, you can refer to star schema table
# You should write respective queries against each varibale

video_play_table_create = (""" CREATE TABLE IF NOT EXISTS videoplay_fact (videoplay_id VARCHAR PRIMARY KEY NOT NULL, \
                                                  start_time VARCHAR NULL , \
                                                  user_id VARCHAR NULL , \
                                                  level varchar NULL , \
                                                  video_id VARCHAR NULL , \
                                                  youtuber_id VARCHAR NULL , \
                                                  session_id VARCHAR NULL , \
                                                  location varchar NULL , \
                                                  user_agent varchar NULL ,\
                                                  FOREIGN KEY (start_time) \
                                                  REFERENCES time_dim (start_time) \
                                                  ON UPDATE CASCADE ON DELETE CASCADE, \
                                                  FOREIGN KEY (user_id) \
                                                  REFERENCES users_dim (user_id) \
                                                  ON UPDATE CASCADE ON DELETE CASCADE,\
                                                  FOREIGN KEY (video_id) \
                                                  REFERENCES videos_dim (video_id) \
                                                  ON UPDATE CASCADE ON DELETE CASCADE, \
                                                  FOREIGN KEY (youtuber_id) \
                                                  REFERENCES youtubers_dim (youtuber_id) \
                                                  ON UPDATE CASCADE ON DELETE CASCADE);""")

users_table_create = (""" CREATE TABLE IF NOT EXISTS users_dim ( user_id VARCHAR PRIMARY KEY, \
                                                                 first_name varchar NULL , \
                                                                 last_name varchar NULL , \
                                                                 gender varchar NULL , \
                                                                 level varchar NULL);""")

videos_table_create = (""" CREATE TABLE IF NOT EXISTS videos_dim (video_id VARCHAR PRIMARY KEY, \
                                                                  title varchar NULL ,\
                                                                  youtuber_id VARCHAR NULL ,\
                                                                  year varchar NULL ,\
                                                                  duration varchar NULL);""")


you_tubers_table_create = (""" CREATE TABLE IF NOT EXISTS youtubers_dim (youtuber_id VARCHAR PRIMARY KEY, \
                                                                         name varchar NULL , \
                                                                         location varchar NULL , \
                                                                         latitude varchar NULL , \
                                                                         longitude varchar NULL ); """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time_dim (start_time VARCHAR PRIMARY KEY, \
                                                              hour varchar NULL , \
                                                              day varchar NULL , \
                                                              week varchar NULL , \
                                                              month varchar NULL , \
                                                              year varchar NULL , \
                                                              weekday varchar NULL);""")

# INSERT RECORDS
# Write queries to insert record to each table, please don't change variable names, you can refer to star schema table
# You should write respective queries against each varibale

video_play_table_insert = ("""INSERT INTO videoplay_fact (videoplay_id,\
                                                          start_time,\
                                                          user_id,\
                                                          level,\
                                                          video_id,\
                                                          youtuber_id,\
                                                          session_id,\
                                                          location,\
                                                          user_agent)\
                                                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""")

users_table_insert = ("""INSERT INTO users_dim (user_id, \
                                                first_name, \
                                                last_name, \
                                                gender, \
                                                level)\
                                                VALUES (%s, %s, %s, %s, %s); """)

videos_table_insert = (""" INSERT INTO videos_dim (video_id, \
                                                   title,\
                                                   youtuber_id,\
                                                   year,\
                                                   duration) \
                                                   VALUES (%s, %s, %s, %s, %s); """)

youtubers_table_insert = (""" INSERT INTO youtubers_dim (youtuber_id, \
                                                         name, \
                                                         location, \
                                                         latitude, \
                                                         longitude) \
                                                         VALUES (%s, %s, %s, %s, %s);""")


time_table_insert = (""" INSERT INTO time_dim (start_time,\
                                               hour,\
                                               day, \
                                               week, \
                                               month, \
                                               year, \
                                               weekday) \
                                               VALUES (%s, %s, %s, %s, %s, %s, %s);""")

# QUERY LISTS


create_table_queries = [users_table_create, time_table_create, you_tubers_table_create,
                        videos_table_create, video_play_table_create]
drop_table_queries = [video_play_table_drop, users_table_drop, videos_table_drop, you_tubers_table_drop, time_table_drop]
