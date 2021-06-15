## Fetch all active litigation
## Create new matters
import datetime
from utils.data_bridge import DataBridge
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--max_records", type=int,
                    help="maximum number of records to pull", default=None, required=False)


if __name__ == "__main__":
    args = parser.parse_args()
    now = datetime.datetime.utcnow().isoformat()
    data_bridge = DataBridge()
    data_bridge.pull_gis_updates(args.max_records)
    data_bridge.push_gis_updates(migrate=True)
    with open(data_bridge.clio_update_log_path, "w") as f:
        f.write(now)
