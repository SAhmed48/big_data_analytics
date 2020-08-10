import psycopg2
from Create_Table_queries import drop_table_queries, create_table_queries


class YoutubeDataWareHouse:
    """
    Class to implement database architecture for creating youtube db.
    """
    db_connect = None
    db_cur = None
    db_conn = None

    def __init__(self):
        return

    def connect_db(self, ):
        self.db_connect = "user='postgres'" \
                          "host='127.0.0.1'" \
                          "password='admin' " \
                          "port='5432'"
        try:
            self.db_conn = psycopg2.connect(self.db_connect)
            self.db_conn.set_session(autocommit=True)
            self.db_cur = self.db_conn.cursor()
        except Exception as err:
            print(err)

    def create_database(self):
        """Creates and connects to youtube  database. Returns cursor and connection to DB"""

        # create youtube database with UTF8 encoding
        self.db_cur.execute("DROP DATABASE IF EXISTS youtubedb")
        self.db_cur.execute("CREATE DATABASE Youtubedb WITH ENCODING 'utf8' TEMPLATE template0")

        # close connection to default database
        self.db_conn.close()

        # connect to youtubedb database
        self.db_conn = psycopg2.connect("host=127.0.0.1 dbname=youtubedb user=postgres password=12345678")
        self.db_cur = self.db_conn.cursor()

        return

    def drop_tables(self):
        """Drops all tables created on the database"""

        for query in drop_table_queries:
            self.db_cur.execute(query)
            self.db_conn.commit()

    def create_tables(self):
        """Created tables defined on the Create_Table_queries script"""

        for query in create_table_queries:
            self.db_cur.execute(query)
            self.db_conn.commit()


def main():
    """ Function to drop and re create youtube database and all related tables.
        Usage: python Create_Table_queries.py
    """

    youtube_db_warehouse = YoutubeDataWareHouse()
    youtube_db_warehouse.connect_db()
    youtube_db_warehouse.create_database()
    youtube_db_warehouse.drop_tables()
    youtube_db_warehouse.create_tables()
    youtube_db_warehouse.db_conn.close()


main()
