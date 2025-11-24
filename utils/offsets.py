import mysql
from mysql import connector

from pyspark.sql import functions as F

class OffsetManager:
    def __init__(self, mysql_conf, spark):
        """
        mysql_conf = {
            "url": "jdbc:mysql://host:3306/db",
            "user": "username",
            "password": "password",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        """
        self.mysql_conf = mysql_conf
        self.spark = spark

    def get_last_offset(self, table):
        """
        Fetches the latest offset for the given table from MySQL.
        Executes:
        select last_offset
        from etl_offsets
        where table_name = '<table>'
        order by inserted_ts desc
        limit 1
        """
        try:
            query = f"(SELECT last_offset FROM etl_offsets WHERE table_name = '{table}' ORDER BY inserted_ts DESC LIMIT 1) as t"
            url = f"jdbc:mysql://{self.mysql_conf['host']}:{self.mysql_conf['port']}/{self.mysql_conf['database']}"

            df = (
                self.spark.read.format("jdbc")
                .option("url", url)
                .option("dbtable", query)
                .option("user", self.mysql_conf["user"])
                .option("password", self.mysql_conf["password"])
                .option("driver", self.mysql_conf["driver"])
                .load()
            )

            if df.count() == 0:
                return 0

            return df.first()["last_offset"]

        except Exception as e:
            print("Error reading last offset:", e)
            return 0

    import mysql.connector

    def update_offset(self, table, pk, df):
        """
        Inserts a new offset row into MySQL etl_offsets using mysql-connector.
        """
        try:
            if df.count() == 0:
                return  # nothing to update

            # Get max primary key from DF
            max_pk = df.agg({pk: "max"}).first()[0]
            print(f"Updating offset: table={table}, last_offset={max_pk}")

            # Connect to MySQL
            conn = mysql.connector.connect(
                host=self.mysql_conf["host"],
                port=self.mysql_conf["port"],
                user=self.mysql_conf["user"],
                password=self.mysql_conf["password"],
                database=self.mysql_conf["database"]
            )
            cursor = conn.cursor()

            # Insert offset record
            sql = """
                INSERT INTO etl_offsets (table_name, last_offset)
                VALUES (%s, %s)
            """
            cursor.execute(sql, (table, max_pk))
            conn.commit()

            cursor.close()
            conn.close()

            print("Offset updated successfully.")

        except Exception as e:
            print("Failed to update offset:", e)


