import json
import os
import re
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

TABLE_NAME = "mcd_core.sal_deployments"
MAX_MONTHS = int(os.getenv("MAX_SQL_AGE_MONTHS", "12"))
EMAIL_TO = "MCD_SC_Cloud_Support_Team@us.mcd.com"
EMAIL_FROM = "no-reply@mcd.com"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ses = boto3.client("ses")

DATE_REGEX = re.compile(r"\d{4}_\d{2}_\d{2}")

def lambda_handler(event, context):
    files = event.get("files", [])
    if not files:
        print("No files in payload")
        return {"status": "NO FILES"}

    grouped = {}
    for f in files:
        path = f["filename"]
        if "sql_data/deployment/" not in path:
            continue
        parts = path.split("/")
        deployment_id = parts[2]  # SCT folder
        grouped.setdefault(deployment_id, []).append(f)

    for deployment_id in sorted(grouped.keys()):
        scripts = sorted(grouped[deployment_id], key=lambda x: x["filename"])
        for script in scripts:
            print(f"Processing file: {script['filename']} under deployment: {deployment_id}")
            status, reason = process_script(deployment_id, script)
            if status == "FAILED":
                break

    return {"status": "COMPLETED"}

def process_script(deployment_id, script):
    script_name = script["filename"].split("/")[-1]
    script_path = script["filename"]

    # Regex match
    match = DATE_REGEX.search(script_name)
    print(f"Regex match for {script_name}: {match}")
    if not match:
        return record_and_notify(deployment_id, script_name, script_path, "IGNORED", "No valid date in filename")

    # Old file check
    script_date = datetime.strptime(match.group(), "%Y_%m_%d")
    if script_date < datetime.utcnow() - timedelta(days=30 * MAX_MONTHS):
        return record_and_notify(deployment_id, script_name, script_path, "IGNORED", "Older than allowed threshold")

    # Already executed
    try:
        existing = table.get_item(Key={"deployment_id": deployment_id, "script_name": script_name})
        if "Item" in existing:
            return record_and_notify(deployment_id, script_name, script_path, "IGNORED", "Already executed")
    except ClientError as e:
        print(f"DynamoDB error: {e}")

    # Execute SQL
    try:
        execute_sql(script["content"])
        record(deployment_id, script_name, script_path, "SUCCESS")
        return "SUCCESS", None
    except Exception as e:
        return record_and_notify(deployment_id, script_name, script_path, "FAILED", str(e))

def execute_sql(content):
    if "INVALID" in content:
        raise Exception("Simulated SQL failure")

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
        print(f"SES failed: {e}")
