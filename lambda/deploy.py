import json
import os
import re
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from collections import defaultdict

# ================= CONFIG =================

TABLE_NAME = "mcd_core.sal_deployments"
MAX_MONTHS = int(os.getenv("MAX_SQL_AGE_MONTHS", "12"))

EMAIL_TO = "MCD_SC_Cloud_Support_Team@us.mcd.com"
EMAIL_FROM = "no-reply@mcd.com"

DATE_REGEX = re.compile(r"\d{4}_\d{2}_\d{2}")
VERSION_REGEX = re.compile(r"_v(\d+)\.sql$")

# ================= AWS =================

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ses = boto3.client("ses")

# ================= HANDLER =================

def lambda_handler(event, context):
    files = event.get("files", [])
    if not files:
        return {"status": "NO FILES"}

    grouped = defaultdict(list)

    # ---- Group by SCT folder
    for f in files:
        path = f["filename"]

        # Expected: sql_data/deployment/SCT-XXXX/file.sql
        parts = path.split("/")
        if len(parts) < 4:
            continue
        if parts[1] != "deployment":
            continue
        if not parts[2].startswith("SCT-"):
            continue

        deployment_id = parts[2]
        grouped[deployment_id].append(f)

    # ---- Execute in deployment order
    for deployment_id in sorted(grouped.keys()):
        scripts = sort_scripts(grouped[deployment_id])

        print(f"🚀 Starting deployment {deployment_id}")

        for script in scripts:
            status, _ = process_script(deployment_id, script)
            if status == "FAILED":
                print(f"❌ Deployment {deployment_id} stopped due to failure")
                break

    return {"status": "COMPLETED"}

# ================= SORTING =================

def sort_scripts(scripts):
    def version_key(script):
        name = script["filename"].split("/")[-1]
        match = VERSION_REGEX.search(name)
        return int(match.group(1)) if match else 9999

    return sorted(scripts, key=version_key)

# ================= PROCESS SCRIPT =================

def process_script(deployment_id, script):
    script_name = script["filename"].split("/")[-1]
    script_path = script["filename"]

    # ---- Validate date
    date_match = DATE_REGEX.search(script_name)
    if not date_match:
        return record_and_notify(
            deployment_id, script_name, script_path,
            "IGNORED", "Filename does not contain date (YYYY_MM_DD)"
        )

    # ---- Validate version
    version_match = VERSION_REGEX.search(script_name)
    if not version_match:
        return record_and_notify(
            deployment_id, script_name, script_path,
            "IGNORED", "Filename does not contain version (_v1, _v2)"
        )

    # ---- Age check
    script_date = datetime.strptime(date_match.group(), "%Y_%m_%d")
    if script_date < datetime.utcnow() - timedelta(days=30 * MAX_MONTHS):
        return record_and_notify(
            deployment_id, script_name, script_path,
            "IGNORED", "Older than allowed threshold"
        )

    # ---- Already executed check
    try:
        existing = table.get_item(
            Key={"deployment_id": deployment_id, "script_name": script_name}
        )
        if "Item" in existing:
            return record_and_notify(
                deployment_id, script_name, script_path,
                "IGNORED", "Already executed"
            )
    except ClientError as e:
        print(f"DynamoDB error: {e}")

    # ---- Execute SQL
    try:
        execute_sql(script["content"])
        record(deployment_id, script_name, script_path, "SUCCESS")
        return "SUCCESS", None
    except Exception as e:
        record_and_notify(
            deployment_id, script_name, script_path,
            "FAILED", str(e)
        )
        return "FAILED", str(e)

# ================= HELPERS =================

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
