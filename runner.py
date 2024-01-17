import argparse
import json
import os
import os.path
import sys
from datetime import datetime

import pendulum

sys.path.insert(0, ".")
from secrets_manager import get_secret

snapchat_creds = get_secret("snapchat")
snowflake_creds = get_secret("snowflake")
sf_schema = os.environ["SNAPCHAT_TARGET_SCHEMA"]


def pretty(*ag):
    d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{d} - {ag[0]}")


def main():
    parser = argparse.ArgumentParser(description="snapchat pipeline")
    parser.add_argument(
        "-s",
        "--start_date",
        help="start date",
        default=pendulum.now().subtract(days=10).to_date_string(),
    )
    parser.add_argument(
        "-e", "--end_date", help="end date", default=pendulum.now().to_date_string()
    )
    parser.add_argument("--catalog", help="catalog json file", default="catalog.json")

    args = parser.parse_args()
    started = datetime.now()

    sf_account = snowflake_creds.get("account")
    sf_username = snowflake_creds.get("username")
    sf_password = snowflake_creds.get("password")
    sf_database = snowflake_creds.get("database")
    sf_warehouse = snowflake_creds.get("warehouse")
    sf_role = snowflake_creds.get("role")

    conf = {
        "client_id": snapchat_creds.get("snapchat_client_id"),
        "client_secret": snapchat_creds.get("snapchat_client_secret"),
        "refresh_token": snapchat_creds.get("snapchat_refresh_token"),
        "swipe_up_attribution_window": "28_DAY",
        "view_attribution_window": "7_DAY",
        "omit_empty": "true",
        "targeting_country_codes": "us, ca, mx",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "user_agent": "tap-snapchat-ads",
        "org_account_ids": [
            {
                "organisation_id": snapchat_creds.get("snapchat_org_id"),
                "ad_accounts": snapchat_creds.get("snapchat_ad_accounts"),
            }
        ],
    }

    tap = "tap-snapchat-ads"
    target = "target-snowflake"
    tap_config = "snapchat_config.json"
    target_config = "snowflake_config.json"

    with open(tap_config, "w", encoding="utf-8") as out:
        json.dump(conf, out)

    snowflake_conf = {
        "user": sf_username,
        "password": sf_password,
        "account": sf_account,
        "database": sf_database,
        "role": sf_role,
        "schema": sf_schema,
        "warehouse": sf_warehouse,
    }

    with open(target_config, "w", encoding="utf-8") as out:
        json.dump(snowflake_conf, out)

    r = os.system(
        f"venv/.{tap}/bin/{tap} --config {tap_config} --catalog {args.catalog} | venv/.venv-target-sf/bin/{target} --config {target_config}"
    )

    if r != 0:
        print(r)
        raise Exception("error occurred!")

    if os.path.exists(tap_config):
        os.remove(tap_config)
    if os.path.exists(target_config):
        os.remove(target_config)

    ended = datetime.now()
    pretty(f"execution time : {ended - started}")


if __name__ == "__main__":
    main()
