import json
import os
import re
import boto3
from datetime import datetime, timedelta

dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
secrets = boto3.client("secretsmanager")

TABLE_NAME = "mcd_core.sal_deployments"
MAX_MONTHS = int(os.getenv("MAX_SQL_AGE_MONTHS", "12"))
SECRET_NAME = os.getenv("DB_SECRET_NAME")

EMAIL_TO = "MCD_SC_Cloud_Support_Team@us.mcd.com"
EMAIL_FROM = "no-reply@mcd.com"

table = dynamodb.Table(TABLE_NAME)

DATE_REGEX = re.compile(r"\d{4}_\d{2}_\d{2}")

def lambda_handler(event, context):
    files = event.get("files", [])
    grouped = {}

    for f in files:
        path = f["filename"]
        if "sql_data/deployment/" not in path:
            continue

        parts = path.split("/")
        deployment_id = parts[2]
        grouped.setdefault(deployment_id, []).append(path)

    for deployment_id in sorted(grouped.keys()):
        scripts = sorted(grouped[deployment_id])

        for script in scripts:
            status, reason = process_script(deployment_id, script)

            if status == "FAILED":
                break

    return {"status": "COMPLETED"}

def process_script(deployment_id, script_path):
    script_name = script_path.split("/")[-1]

    # Ignore: no date
    if not DATE_REGEX.search(script_name):
        return record_and_notify(deployment_id, script_name, script_path, "IGNORED", "No valid date in filename")

    # Ignore: old file
    date_str = DATE_REGEX.search(script_name).group()
    script_date = datetime.strptime(date_str, "%Y_%m_%d")
    if script_date < datetime.utcnow() - timedelta(days=30 * MAX_MONTHS):
        return record_and_notify(deployment_id, script_name, script_path, "IGNORED", "Older than allowed threshold")

    # Ignore: already executed
    existing = table.get_item(
        Key={"deployment_id": deployment_id, "script_name": script_name}
    )
    if "Item" in existing:
        return "IGNORED", "Already executed"

    try:
        execute_sql(script_path)
        record(deployment_id, script_name, script_path, "SUCCESS")
        return "SUCCESS", None
    except Exception as e:
        record_and_notify(deployment_id, script_name, script_path, "FAILED", str(e))
        return "FAILED", str(e)

def execute_sql(script_path):
    # Placeholder for DB execution
    # Fetch secret → connect → execute → commit
    pass

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
