import sys
import csv
from pathlib import Path

DEFAULT = Path('output/montana-migration.csv')
# calculate total number of inbound new residents from all states (and regions like `Asia` or `Europe`)

def parse_int(value):
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def sum_inbound(path: Path):
    if not path.exists():
        print(f"File not found: {path}")
        return 2

    total = 0
    rows = 0
    skipped = 0

    with path.open(newline='') as fh:
        reader = csv.DictReader(fh)
        if 'inboundFromState' not in reader.fieldnames:
            print("CSV does not contain header 'inboundFromState'. Available headers:", reader.fieldnames)
            return 3

        for r in reader:
            rows += 1
            val = parse_int(r.get('inboundFromState', ''))
            if val is None:
                skipped += 1
                continue
            total += val

    print(f"File: {path}")
    print(f"Rows processed: {rows}")
    print(f"Skipped rows (non-numeric): {skipped}")
    print(f"Total inboundFromState: {total}")
    return 0


if __name__ == '__main__':
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    code = sum_inbound(path)
    sys.exit(code)
