import json
import os
import re
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# ================= CONFIG =================
TABLE_NAME = "mcd_core.sal_deployments"
MAX_MONTHS = int(os.getenv("MAX_SQL_AGE_MONTHS", "12"))

EMAIL_TO = "MCD_SC_Cloud_Support_Team@us.mcd.com"
EMAIL_FROM = "no-reply@mcd.com"

# =========================================
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ses = boto3.client("ses")

# STRICT filename rule: *_YYYY_MM_DD_vN.sql
DATE_REGEX = re.compile(r"_(\d{4}_\d{2}_\d{2})_v\d+\.sql$")

# ================= LAMBDA =================
def lambda_handler(event, context):
    files = event.get("files", [])
    if not files:
        return {"status": "NO_FILES"}

    grouped = {}

    for f in files:
        path = f["filename"]

        if not path.startswith("sql_data/deployment/"):
            continue

        parts = path.split("/")
        if len(parts) < 4:
            continue

        deployment_id = parts[2]
        grouped.setdefault(deployment_id, []).append(f)

    # Execute deployment folders in order
    for deployment_id in sorted(grouped.keys()):
        print(f"\n=== Deployment {deployment_id} ===")

        scripts = sorted(grouped[deployment_id], key=lambda x: x["filename"])

        for script in scripts:
            status, _ = process_script(deployment_id, script)

            # STOP execution on failure for THIS deployment
            if status == "FAILED":
                print(f"Stopping deployment {deployment_id} due to FAILURE")
                break

    return {"status": "COMPLETED"}

# ================= CORE LOGIC =================
def process_script(deployment_id, script):
    script_path = script["filename"]
    script_name = script_path.split("/")[-1]

    print(f"Processing: {script_name}")

    # 1️⃣ Filename validation
    match = DATE_REGEX.search(script_name)
    if not match:
        return record_and_notify(
            deployment_id, script_name, script_path,
            "IGNORED", "Filename does not follow *_YYYY_MM_DD_vN.sql format"
        )

    # 2️⃣ Date validation
    script_date = datetime.strptime(match.group(1), "%Y_%m_%d")
    if script_date < datetime.utcnow() - timedelta(days=30 * MAX_MONTHS):
        return record_and_notify(
            deployment_id, script_name, script_path,
            "IGNORED", "SQL file older than allowed threshold"
        )

    # 3️⃣ Already executed check
    try:
        existing = table.get_item(
            Key={"deployment_id": deployment_id, "script_name": script_name}
        )
        if "Item" in existing:
            print(f"Already executed: {script_name}")
            return "IGNORED", "Already executed"
    except ClientError as e:
        print(f"DynamoDB error: {e}")

    # 4️⃣ Execute SQL
    try:
        execute_sql(script["content"])
        record(deployment_id, script_name, script_path, "SUCCESS")
        return "SUCCESS", None

    except Exception as e:
        return record_and_notify(
            deployment_id, script_name, script_path,
            "FAILED", str(e)
        )

# ================= HELPERS =================
def execute_sql(content):
    # Replace with real execution logic
    if "INVALID" in content:
        raise Exception("SQL execution failed")

def record(deployment_id, script_name, path, status, reason=None):
    item = {
        "deployment_id": deployment_id,
        "script_name": script_name,
        "script_path": path,
        "deployed_at": datetime.utcnow().isoformat(),
        "status": status
    }
    if reason:
        item["failure_reason"] = reason

    table.put_item(Item=item)

def record_and_notify(deployment_id, script_name, path, status, reason):
    record(deployment_id, script_name, path, status, reason)
    send_email(deployment_id, script_name, path, status, reason)
    return status, reason

def send_email(deployment_id, script_name, path, status, reason):
    try:
        body = f"""
Deployment ID : {deployment_id}
Script Name  : {script_name}
Script Path  : {path}
Status       : {status}
Reason       : {reason}
"""
        ses.send_email(
            Source=EMAIL_FROM,
            Destination={"ToAddresses": [EMAIL_TO]},
            Message={
                "Subject": {"Data": f"[SQL DEPLOYMENT] {status}"},
                "Body": {"Text": {"Data": body}}
            }
        )
    except Exception as e:
        print(f"SES error: {e}")
