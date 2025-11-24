import traceback
from pyspark.sql.functions import col

class IngestStage:
    def __init__(self, spark, mysql_conf, offset_manager, logger):
        self.spark = spark
        self.mysql_conf = mysql_conf
        self.offset_manager = offset_manager
        self.logger = logger

    def _read(self, table, where=None):
        try:
            url = f"jdbc:mysql://{self.mysql_conf['host']}:{self.mysql_conf['port']}/{self.mysql_conf['database']}"

            query = f"(SELECT * FROM {table}"
            if where:
                query += f" WHERE {where}"
            query += ") as t"
            print(url)
            print(query)
            print(self.mysql_conf)
            return (self.spark.read.format("jdbc")
                    .option("url", url)
                    .option("user", self.mysql_conf["user"])
                    .option("password", self.mysql_conf["password"])
                    .option("url", url)
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("dbtable", query)
                    .load())
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
            raise

    def load_incremental(self, table, pk):
        last = self.offset_manager.get_last_offset(table)
        where = f"{pk} > {last}"
        df = self._read(table, where)
        self.logger.info(f"Loaded {df.count()} new rows from {table}")
        return df
