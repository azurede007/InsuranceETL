from utils.logger import get_logger
from utils.spark_session import get_spark
from utils.offsets import OffsetManager
from stages.ingest import IngestStage
from stages.transform import TransformStage
from stages.writeback import WriteBackStage
import yaml, sys, traceback

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

def main():
    logger = get_logger("InsuranceETL")
    try:
        config = load_config("config.yml")
        spark = get_spark("Insurance_ETL_Pipeline")
        offset_mgr = OffsetManager(config["mysql"], spark)

        ingest = IngestStage(spark, config["mysql"], offset_mgr, logger)
        transform = TransformStage(logger)
        writeback = WriteBackStage(config["mysql"], logger)

        claims = ingest.load_incremental("claims", "claim_id")
        customers = ingest.load_incremental("customers", "customer_id")

        policies = ingest.load_incremental("policies", "policy_id")
        payments = ingest.load_incremental("payments", "payment_id")

        curated = transform.enrich(claims, customers, policies, payments)
        metrics = transform.generate_metrics(curated)

        writeback.write_metrics(metrics)

        offset_mgr.update_offset("claims", "claim_id", claims)
        offset_mgr.update_offset("customers", "customer_id", customers)
        offset_mgr.update_offset("policies", "policy_id", policies)
        offset_mgr.update_offset("payments", "payment_id", payments)

        logger.info("ETL Completed Successfully")

    except Exception as e:
        logger.error("ETL Failure: " + str(e))
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
