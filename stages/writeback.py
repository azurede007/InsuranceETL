class WriteBackStage:
    def __init__(self, mysql_conf, logger):
        self.mysql_conf = mysql_conf
        self.logger = logger

    def write_metrics(self, df):
        url = f"jdbc:mysql://{self.mysql_conf['host']}:{self.mysql_conf['port']}/{self.mysql_conf['database']}"
        (df.write.format("jdbc")
         .option("user", self.mysql_conf["user"])
         .option("password", self.mysql_conf["password"])
         .option("url", url)
         .option("dbtable", "insurance_metrics")
         .mode("append").save())

        self.logger.info("Metrics written to MySQL")
