#!/usr/bin/env python3
"""Shopping Sessions: reference implementation.

Written straight from rules.md, in the order the rules are written. Its job is
to be obviously right, not fast. The only concession to size is partitioning:
rows are spread over bucket files in SCRATCH_DIR by an exact key (event_id for
dedup, person for sessions), and each bucket is processed on its own. Every
rule groups by one of those keys, so partitioning cannot change the answer.

Env: DATA_DIR, OUTPUT_DIR, SCRATCH_DIR, NCPU (worker processes), BUCKETS.
Logs go to stderr only.
"""
import bisect
import datetime
import json
import os
import shutil
import sys
import time
import zlib
from multiprocessing import get_context

DAY_MS = 86_400_000
SESSION_GAP_MS = 1_800_000          # a gap OVER this starts a new session
CLICK_REACH_MS = 7 * DAY_MS         # a click credits sessions up to 7 days later
LATE_MS = DAY_MS                    # ingest_ts - ts OVER this is late
PRICE_MAX = 10_000_000
NEVER = 1 << 200                    # an unparseable ingest_ts never wins dedup

EVENT_FIELDS = 7   # event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
CLICK_FIELDS = 4   # click_id,user_id,campaign_id,ts
LINK_FIELDS = 2    # user_id_a,user_id_b
EVENT_TYPES = {b"view", b"cart", b"purchase"}
UNKNOWN_CATEGORY = b"unknown"
ORGANIC = b"organic"

START = time.time()


def log(*parts):
    print(f"[ref {time.time() - START:7.1f}s]", *parts, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- CSV dialect

def parse_line(line, field_count):
    """One line (no trailing \\n) -> list of values (bytes, or None for null),
    or None if the line is malformed.

    Only \\n ends a line, so \\r is an ordinary character and stays in its
    field. Every comma separates fields. A field may be wrapped in double
    quotes, which are syntax; a quote anywhere else makes the line malformed.
    An empty field is null. Spaces are kept. An empty line is malformed.
    """
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != field_count:
        return None
    values = []
    for field in fields:
        if b'"' in field:
            if len(field) < 2 or field[0] != 0x22 or field[-1] != 0x22 or b'"' in field[1:-1]:
                return None
            field = field[1:-1]
        values.append(field if field != b"" else None)
    return values


def read_rows(path, field_count):
    """Every data row of one file. The first line is the header and is
    skipped; only the first line. Malformed lines are dropped."""
    with open(path, "rb") as handle:
        first = True
        for line in handle:            # binary mode splits on b"\n" only
            if line.endswith(b"\n"):
                line = line[:-1]
            if first:
                first = False
                continue
            values = parse_line(line, field_count)
            if values is not None:
                yield values


def csv_files(folder):
    if not os.path.isdir(folder):
        return []
    return sorted(
        os.path.join(folder, name)
        for name in os.listdir(folder)
        if name.endswith(".csv") and os.path.isfile(os.path.join(folder, name))
    )


# ---------------------------------------------------------------- timestamps

def is_leap(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def days_from_civil(year, month, day):
    """Days since 1970-01-01 for a proleptic Gregorian date (Hinnant)."""
    year -= month <= 2
    era = year // 400
    year_of_era = year - era * 400
    day_of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146_097 + day_of_era - 719_468


def ascii_digits(value):
    # bytes.isdigit() is True only for non-empty runs of ASCII 0-9.
    return value.isdigit()


def parse_timestamp(value):
    """F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, or F3 digits
    (epoch ms, leading zeros allowed). Returns epoch ms (int) or None."""
    if value is None:
        return None
    if ascii_digits(value):                              # F3
        return int(value)
    if len(value) == 20:                                 # F1
        if value[19] != ord("Z"):
            return None
        offset_ms = 0
    elif len(value) == 25:                               # F2
        sign = value[19]
        if sign not in (ord("+"), ord("-")) or value[22] != ord(":"):
            return None
        hours, minutes = value[20:22], value[23:25]
        if not (ascii_digits(hours) and ascii_digits(minutes)):
            return None
        hours, minutes = int(hours), int(minutes)
        if hours > 23 or minutes > 59:
            return None
        offset_ms = (hours * 60 + minutes) * 60_000
        if sign == ord("-"):
            offset_ms = -offset_ms
    else:
        return None
    if (value[4] != ord("-") or value[7] != ord("-") or value[10] != ord("T")
            or value[13] != ord(":") or value[16] != ord(":")):
        return None
    parts = (value[0:4], value[5:7], value[8:10], value[11:13], value[14:16], value[17:19])
    if not all(ascii_digits(part) for part in parts):
        return None
    year, month, day, hour, minute, second = (int(part) for part in parts)
    if not 1 <= month <= 12:
        return None
    month_days = 29 if month == 2 and is_leap(year) else DAYS_IN_MONTH[month - 1]
    if not 1 <= day <= month_days or hour > 23 or minute > 59 or second > 59:
        return None
    local_ms = (days_from_civil(year, month, day) * 86_400
                + hour * 3_600 + minute * 60 + second) * 1_000
    return local_ms - offset_ms                          # subtract the offset -> UTC


# ---------------------------------------------------------------- linked IDs

def resolve_people(data_dir):
    """user_id -> person name (byte-wise smallest ID of the person) for every
    ID in a surviving link line. IDs not in the map are people on their own."""
    parent = {}
    size = {}

    def find(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]   # path halving
            node = parent[node]
        return node

    def add(node):
        if node not in parent:
            parent[node] = node
            size[node] = 1

    for path in csv_files(os.path.join(data_dir, "links")):
        for a, b in read_rows(path, LINK_FIELDS):
            if a is None or b is None:               # either field empty: drop
                continue
            add(a)
            add(b)
            root_a, root_b = find(a), find(b)
            if root_a == root_b:
                continue
            if size[root_a] < size[root_b]:
                root_a, root_b = root_b, root_a
            parent[root_b] = root_a
            size[root_a] += size[root_b]

    smallest = {}
    for node in parent:
        root = find(node)
        if root not in smallest or node < smallest[root]:   # bytes compare = byte-wise
            smallest[root] = node
    return {node: smallest[find(node)] for node in parent}


# ---------------------------------------------------------------- small inputs

def load_lookup(path):
    """products.csv / campaigns.csv: first column -> second column."""
    table = {}
    if os.path.isfile(path):
        for key, value in read_rows(path, 2):
            if key is not None and key not in table:
                table[key] = value
    return table


def load_window(data_dir):
    with open(os.path.join(data_dir, "manifest.json"), "rb") as handle:
        manifest = json.load(handle)
    start = parse_timestamp(manifest["window_start"].encode())
    end = parse_timestamp(manifest["window_end"].encode())
    if start is None or end is None:
        raise ValueError(f"bad manifest window: {manifest}")
    return start, end


# ---------------------------------------------------------------- bucket files
# Parsed values never hold a comma, quote or newline (the parser guarantees it),
# and null <-> empty, so `b",".join(values)` round-trips exactly.

def bucket_of(key, buckets):
    return zlib.crc32(key) % buckets


class BucketWriter:
    def __init__(self, folder, name, buckets):
        self.paths = [os.path.join(folder, f"{bucket:04d}", name) for bucket in range(buckets)]
        self.pending = [[] for _ in range(buckets)]

    def add(self, bucket, line):
        pending = self.pending[bucket]
        pending.append(line)
        if len(pending) >= 4096:
            self.flush(bucket)

    def flush(self, bucket):
        if self.pending[bucket]:
            with open(self.paths[bucket], "ab") as handle:
                handle.write(b"".join(self.pending[bucket]))
            self.pending[bucket] = []

    def close(self):
        for bucket in range(len(self.pending)):
            self.flush(bucket)


def bucket_lines(folder, bucket):
    directory = os.path.join(folder, f"{bucket:04d}")
    for name in sorted(os.listdir(directory)):
        with open(os.path.join(directory, name), "rb") as handle:
            for line in handle:
                yield line[:-1].split(b",")


def null(value):
    return value if value != b"" else None


# ---------------------------------------------------------------- the job
# Shared read-only state, set in the parent before the worker pool forks.
STATE = {}


def split_by_id(task):
    """Parse one input file; send each row with a non-empty id (field 0) to
    the bucket of that id."""
    index, path, field_count, folder = task
    buckets = STATE["buckets"]
    writer = BucketWriter(folder, f"{index:05d}.csv", buckets)
    rows = 0
    for values in read_rows(path, field_count):
        if values[0] is None:                    # empty event_id / click_id: drop
            continue
        writer.add(bucket_of(values[0], buckets),
                   b",".join(value or b"" for value in values) + b"\n")
        rows += 1
    writer.close()
    return rows


def events_bucket(bucket):
    """Events steps 2-4 for every event_id in this bucket, then send the
    survivors to their person's bucket."""
    window_start, window_end = STATE["window"]
    people, products = STATE["people"], STATE["products"]

    # Step 2: dedup on event_id, keeping the copy with the smallest ingest_ts.
    # Copies with equal ingest_ts are identical, so the first one seen is kept.
    kept = {}
    for values in bucket_lines(STATE["events_by_id"], bucket):
        ingest = parse_timestamp(null(values[3]))
        ingest = NEVER if ingest is None else ingest
        current = kept.get(values[0])
        if current is None or ingest < current[0]:
            kept[values[0]] = (ingest, values)

    writer = BucketWriter(STATE["events_by_person"], f"{bucket:05d}.csv", STATE["buckets"])
    survivors = 0
    for ingest, values in kept.values():
        _event_id, user_id, ts, _ingest_ts, event_type, product_id, price = map(null, values)
        # Step 3: validate the kept copy.
        if user_id is None or product_id is None or event_type not in EVENT_TYPES:
            continue
        ts = parse_timestamp(ts)
        if ts is None or not window_start <= ts < window_end:
            continue
        purchase = event_type == b"purchase"
        if purchase:
            if price is None or not ascii_digits(price):
                continue
            price = int(price)
            if not 1 <= price <= PRICE_MAX:
                continue
        else:
            price = 0                            # ignored for view and cart
        # Step 4: drop late events (exactly 24 h is kept; ingest before ts is kept).
        if ingest == NEVER or ingest - ts > LATE_MS:
            continue
        person = people.get(user_id, user_id)
        category = products.get(product_id, UNKNOWN_CATEGORY)
        writer.add(bucket_of(person, STATE["buckets"]),
                   b"%s,%d,%d,%d,%s\n" % (person, ts, purchase, price, category))
        survivors += 1
    writer.close()
    return len(kept), survivors


def clicks_bucket(bucket):
    """Clicks steps 2-3 for every click_id in this bucket, then send the
    valid ones to their person's bucket."""
    window_start, window_end = STATE["window"]
    people, campaigns = STATE["people"], STATE["campaigns"]

    # Step 2: dedup on click_id. Copies have identical values: keep any one.
    kept = {}
    for values in bucket_lines(STATE["clicks_by_id"], bucket):
        kept.setdefault(values[0], values)

    writer = BucketWriter(STATE["clicks_by_person"], f"{bucket:05d}.csv", STATE["buckets"])
    valid = 0
    for values in kept.values():
        click_id, user_id, campaign_id, ts = map(null, values)
        # Step 3: validate.
        if user_id is None or campaign_id is None or campaign_id not in campaigns:
            continue
        ts = parse_timestamp(ts)
        if ts is None or not window_start - CLICK_REACH_MS <= ts < window_end:
            continue
        person = people.get(user_id, user_id)
        writer.add(bucket_of(person, STATE["buckets"]),
                   b"%s,%d,%s,%s\n" % (person, ts, click_id, campaign_id))
        valid += 1
    writer.close()
    return len(kept), valid


def people_bucket(bucket):
    """Sessionize every person in this bucket, credit and assist each session,
    and aggregate. Writes this bucket's user_daily rows; returns its
    campaign_daily partial counts."""
    events = {}
    for person, ts, purchase, price, category in bucket_lines(STATE["events_by_person"], bucket):
        events.setdefault(person, []).append((int(ts), purchase == b"1", int(price), category))
    clicks = {}
    if os.path.isdir(os.path.join(STATE["clicks_by_person"], f"{bucket:04d}")):
        for person, ts, click_id, campaign in bucket_lines(STATE["clicks_by_person"], bucket):
            clicks.setdefault(person, []).append((int(ts), click_id, campaign))

    user_daily = {}       # (person, day) -> [sessions, events, revenue, {category: revenue}]
    campaign_daily = {}   # (campaign, day) -> [sessions, purchasing, revenue, assisted]
    session_count = 0

    for person, person_events in events.items():
        # Events step 5: sort by ts; a gap OVER 30 min starts a new session.
        # Equal timestamps have a gap of 0, so they always share a session.
        person_events.sort(key=lambda event: event[0])
        sessions = []
        previous_ts = None
        for event in person_events:
            if previous_ts is None or event[0] - previous_ts > SESSION_GAP_MS:
                sessions.append([])
            sessions[-1].append(event)
            previous_ts = event[0]

        # Clicks sorted by (ts, click_id): the latest ts wins, then the
        # byte-wise smallest click_id.
        person_clicks = sorted(clicks.get(person, ()), key=lambda click: (click[0], click[1]))
        click_times = [click[0] for click in person_clicks]

        for session in sessions:
            session_count += 1
            start = session[0][0]
            day = start // DAY_MS                 # Events step 6: UTC date of the start
            revenue = sum(event[2] for event in session if event[1])
            purchased = any(event[1] for event in session)

            row = user_daily.setdefault((person, day), [0, 0, 0, {}])
            row[0] += 1
            row[1] += len(session)
            row[2] += revenue
            for _ts, purchase, price, category in session:
                if purchase:
                    row[3][category] = row[3].get(category, 0) + price

            # Clicks step 4: qualifying clicks have start - 7 d <= ts <= start.
            low = bisect.bisect_left(click_times, start - CLICK_REACH_MS)
            high = bisect.bisect_right(click_times, start)
            if high > low:
                latest = click_times[high - 1]
                first_at_latest = bisect.bisect_left(click_times, latest, low, high)
                credited = person_clicks[first_at_latest][2]
                # Clicks step 7: every campaign among the same clicks assists once.
                assisting = {person_clicks[at][2] for at in range(low, high)}
            else:
                credited = ORGANIC                # Clicks step 5
                assisting = set()

            row = campaign_daily.setdefault((credited, day), [0, 0, 0, 0])
            row[0] += 1
            row[1] += purchased
            row[2] += revenue
            for campaign in assisting:
                campaign_daily.setdefault((campaign, day), [0, 0, 0, 0])[3] += 1

    if user_daily:
        write_user_daily(user_daily, os.path.join(STATE["output"], "user_daily",
                                                  f"part-{bucket:05d}.parquet"))
    return campaign_daily, session_count


# ---------------------------------------------------------------- output

def to_date(day):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=day)


def top_category(revenue_by_category):
    """Highest summed purchase price; a tie goes to the byte-wise smallest;
    None with no purchases."""
    if not revenue_by_category:
        return None
    best = max(revenue_by_category.values())
    return min(category for category, revenue in revenue_by_category.items() if revenue == best)


def write_user_daily(rows, path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    keys = list(rows)
    table = pa.table({
        "user_id": pa.array([person.decode("utf-8") for person, _ in keys], pa.string()),
        "day": pa.array([to_date(day) for _, day in keys], pa.date32()),
        "sessions": pa.array([rows[key][0] for key in keys], pa.int64()),
        "events": pa.array([rows[key][1] for key in keys], pa.int64()),
        "revenue_cents": pa.array([rows[key][2] for key in keys], pa.int64()),
        "top_category": pa.array([
            None if (category := top_category(rows[key][3])) is None else category.decode("utf-8")
            for key in keys], pa.string()),
    })
    pq.write_table(table, path)


def write_campaign_daily(rows, channels, path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    keys = list(rows)

    def channel(campaign):
        return ORGANIC if campaign == ORGANIC else channels[campaign]

    table = pa.table({
        "campaign_id": pa.array([campaign.decode("utf-8") for campaign, _ in keys], pa.string()),
        "channel": pa.array([channel(campaign).decode("utf-8") for campaign, _ in keys], pa.string()),
        "day": pa.array([to_date(day) for _, day in keys], pa.date32()),
        "sessions": pa.array([rows[key][0] for key in keys], pa.int64()),
        "purchasing_sessions": pa.array([rows[key][1] for key in keys], pa.int64()),
        "revenue_cents": pa.array([rows[key][2] for key in keys], pa.int64()),
        "assisted_sessions": pa.array([rows[key][3] for key in keys], pa.int64()),
    })
    pq.write_table(table, path)


def write_empty_user_daily(path):
    write_user_daily({}, path)


def prepare_output(output_dir):
    for table in ("user_daily", "campaign_daily"):
        folder = os.path.join(output_dir, table)
        os.makedirs(folder, exist_ok=True)
        for name in os.listdir(folder):
            if name.endswith(".parquet"):
                os.remove(os.path.join(folder, name))


# ---------------------------------------------------------------- main

def main():
    data_dir = os.environ.get("DATA_DIR") or (sys.argv[1] if len(sys.argv) > 1 else "/data")
    output_dir = os.environ.get("OUTPUT_DIR") or (sys.argv[2] if len(sys.argv) > 2 else "/output")
    scratch_dir = os.environ.get("SCRATCH_DIR") or "/scratch"
    workers = max(1, int(os.environ.get("NCPU") or os.cpu_count() or 1))
    buckets = max(1, int(os.environ.get("BUCKETS") or 128))

    work = os.path.join(scratch_dir, f"reference-work-{os.getpid()}")
    shutil.rmtree(work, ignore_errors=True)
    STATE.update(
        buckets=buckets,
        output=output_dir,
        events_by_id=os.path.join(work, "events_by_id"),
        clicks_by_id=os.path.join(work, "clicks_by_id"),
        events_by_person=os.path.join(work, "events_by_person"),
        clicks_by_person=os.path.join(work, "clicks_by_person"),
    )
    for name in ("events_by_id", "clicks_by_id", "events_by_person", "clicks_by_person"):
        for bucket in range(buckets):
            os.makedirs(os.path.join(STATE[name], f"{bucket:04d}"), exist_ok=True)
    prepare_output(output_dir)

    # Small inputs and links first: an event or click is its person's as soon as it is parsed.
    STATE["window"] = load_window(data_dir)
    STATE["products"] = load_lookup(os.path.join(data_dir, "products.csv"))
    STATE["campaigns"] = load_lookup(os.path.join(data_dir, "campaigns.csv"))
    STATE["people"] = resolve_people(data_dir)
    log(f"window {STATE['window']}, {len(STATE['products'])} products, "
        f"{len(STATE['campaigns'])} campaigns, {len(STATE['people'])} linked ids, "
        f"{workers} workers, {buckets} buckets")

    event_files = csv_files(os.path.join(data_dir, "events"))
    click_files = csv_files(os.path.join(data_dir, "clicks"))
    tasks = ([(index, path, EVENT_FIELDS, STATE["events_by_id"])
              for index, path in enumerate(event_files)]
             + [(index, path, CLICK_FIELDS, STATE["clicks_by_id"])
                for index, path in enumerate(click_files)])
    # Largest files first, so one big file does not finish last.
    tasks.sort(key=lambda task: -os.path.getsize(task[1]))

    with get_context("fork").Pool(workers) as pool:
        parsed = sum(pool.imap_unordered(split_by_id, tasks))
        log(f"parsed {parsed} rows with an id from {len(event_files)} event and "
            f"{len(click_files)} click files")

        event_counts = pool.map(events_bucket, range(buckets))
        log(f"events: {sum(c[0] for c in event_counts)} distinct ids, "
            f"{sum(c[1] for c in event_counts)} survive")
        click_counts = pool.map(clicks_bucket, range(buckets))
        log(f"clicks: {sum(c[0] for c in click_counts)} distinct ids, "
            f"{sum(c[1] for c in click_counts)} valid")

        campaign_daily = {}
        sessions = 0
        for partial, count in pool.imap_unordered(people_bucket, range(buckets)):
            sessions += count
            for key, counts in partial.items():
                row = campaign_daily.setdefault(key, [0, 0, 0, 0])
                for at in range(4):
                    row[at] += counts[at]
        log(f"{sessions} sessions, {len(campaign_daily)} campaign_daily rows")

    user_folder = os.path.join(output_dir, "user_daily")
    if not any(name.endswith(".parquet") for name in os.listdir(user_folder)):
        write_empty_user_daily(os.path.join(user_folder, "part-00000.parquet"))
    write_campaign_daily(campaign_daily, STATE["campaigns"],
                         os.path.join(output_dir, "campaign_daily", "part-00000.parquet"))
    shutil.rmtree(work, ignore_errors=True)
    log("done")


if __name__ == "__main__":
    main()
