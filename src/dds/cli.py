"""DDS commands; source JSON and credentials are never printed."""

import argparse
import json
import logging

from . import DdsError
from .loader import initialize_dds, load_dds, preview_dds, validate_dds


def main(argv=None):
    parser = argparse.ArgumentParser(description="Normalize confirmed GitHub STG into DDS")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Apply DDS migrations without fetching GitHub")
    for name in ("load", "preview", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--batch-id", action="append", required=name != "validate")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "init-db":
            initialize_dds()
            result = {"status": "schema_ready"}
        elif args.command == "validate":
            result = validate_dds(expected_batch_ids=args.batch_id)
        elif args.command == "load":
            result = load_dds(args.batch_id)
        else:
            result = preview_dds(args.batch_id)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except DdsError as exc:
        logging.error("DDS quality check failed: %s", exc)
    except Exception as exc:
        logging.error("DDS command failed (%s)", type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
