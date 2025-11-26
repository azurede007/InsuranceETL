#!/bin/bash
set -e

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------
REPO_URL="https://github.com/azurede007/InsuranceETL"
BUCKET_PATH="gs://iz-insureproject/insurance-etl"
BATCH_NAME="insurance-010"
REGION="us-central1"
RUNTIME_VERSION="2.3"
WHEEL_NAME="mysql_connector_python-9.5.0-py2.py3-none-any.whl"

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
