## Get new documents
from utils.data_bridge import DataBridge
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--max_records", type=int,
                    help="maximum number of records to pull", default=None, required=False)
parser.add_argument("--migrate", type=bool,
                    help="migration?", default=False, required=False)


if __name__ == "__main__":
    args = parser.parse_args()
    data_bridge = DataBridge()
    data_bridge.pull_gis_updates(migrate=args.migrate, max_records=args.max_records)
