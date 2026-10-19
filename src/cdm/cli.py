"""Initialize, publish and validate curated marts without querying STG."""

from __future__ import annotations

import argparse
import json
import logging

from . import CdmError
from .loader import initialize_cdm, load_cdm, validate_cdm


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build GitHub Issues CDM from ready DDS")
    parser.add_argument("command", choices=("init-db", "load", "validate"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "init-db":
            result = initialize_cdm()
        elif args.command == "load":
            result = load_cdm()
        else:
            result = validate_cdm()
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except CdmError as exc:
        logging.error("CDM quality check failed: %s", exc)
    except Exception as exc:
        logging.error("CDM command failed (%s)", type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
