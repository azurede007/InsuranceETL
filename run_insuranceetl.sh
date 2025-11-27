#!/bin/bash
set -e

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------
REPO_URL="https://github.com/azurede007/InsuranceETL"
BUCKET_PATH="gs://iz-insureproject/insurance-etl"
BATCH_NAME="insurance-012"
REGION="us-central1"
RUNTIME_VERSION="2.3"
WHEEL_NAME="mysql_connector_python-9.5.0-py2.py3-none-any.whl"

# MySQL Credentials
MYSQL_HOST="34.66.249.221"
MYSQL_PORT=3306
MYSQL_DB="insurancedb"
MYSQL_USER="root"
MYSQL_PASS="root"

echo "---------------------------------------------"
echo " Cleaning old directory..."
echo "---------------------------------------------"
rm -rf InsuranceETL

echo "---------------------------------------------"
echo " Cloning repository..."
echo "---------------------------------------------"
git clone "$REPO_URL"

cd InsuranceETL

echo "---------------------------------------------"
echo " Running DB SQL scripts from sql/db_scripts.sql ..."
echo "---------------------------------------------"

if [ -f "sql/db_scripts.sql" ]; then
    mysql -h "$MYSQL_HOST" \
          -P "$MYSQL_PORT" \
          -u "$MYSQL_USER" \
          -p"$MYSQL_PASS" \
          "$MYSQL_DB" < sql/db_scripts.sql

    echo "SQL scripts executed successfully."
else
    echo "ERROR: sql/db_scripts.sql not found."
    exit 1
fi

echo "---------------------------------------------"
echo " Packaging project folders into Insurance-pkg.zip..."
echo "---------------------------------------------"
zip -r Insurance-pkg.zip utils/ stages/

echo "---------------------------------------------"
echo " Downloading mysql-connector-python wheel (platform independent)..."
echo "---------------------------------------------"
pip download mysql-connector-python \
    --platform any \
    --only-binary=:all: \
    --no-deps \
    -d .

echo "---------------------------------------------"
echo " Uploading project files to GCS bucket..."
echo "---------------------------------------------"
gsutil -m cp -r . "$BUCKET_PATH"

echo "---------------------------------------------"
echo " Submitting Dataproc Serverless PySpark Batch Job..."
echo "---------------------------------------------"
gcloud dataproc batches submit pyspark "$BUCKET_PATH/main.py" \
    --region="$REGION" \
    --batch="$BATCH_NAME" \
    --version="$RUNTIME_VERSION" \
    --py-files="$BUCKET_PATH/Insurance-pkg.zip,$BUCKET_PATH/$WHEEL_NAME" \
    --files="$BUCKET_PATH/conf/config.yml" \
    --jars "$BUCKET_PATH/mysql-connector-j-8.0.33.jar"

echo "---------------------------------------------"
echo "Job Submitted Successfully!"
echo "---------------------------------------------"
