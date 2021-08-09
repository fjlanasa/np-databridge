## authenticate
## get unprocessed updates
## update next court date, update next court notes

from utils.data_bridge import DataBridge
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--migrate", type=bool,
                    help="migration?", default=False, required=False)


if __name__ == "__main__":
    args = parser.parse_args()
    data_bridge = DataBridge()
    data_bridge.push_gis_updates(migrate=args.migrate)
