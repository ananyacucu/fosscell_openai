        os.close(fd)

    # ---- phase 2: ranks, in parallel, biggest groups first ----
    keys = sorted(groups)
    total = sum(len(p) for g in groups.values() for p in g[1])
    results = {}
    nproc = 1 if single else min(cpu_budget(), len(keys))
    if nproc <= 1 or total < (1 << 20):
        for key in keys:
            results[key] = ranks_of(groups[key][1])
    else:
        order = sorted(keys, key=lambda k: -sum(len(p) for p in groups[k][1]))
        bins = [[] for _ in range(nproc)]
        load = [0] * nproc
        for key in order:                                   # longest-processing-time first
            i = load.index(min(load))
            bins[i].append(key)
            load[i] += sum(len(p) for p in groups[key][1]) or 1

        def make(mine):
            def run():
                return marshal.dumps({k: ranks_of(groups[k][1]) for k in mine})
            return run
        children = [spawn(make(b)) for b in bins]
        for blob in collect(children):
            results.update(marshal.loads(blob))

    lines = []
    for key in keys:
        h, e = key
        errs = groups[key][0]
        n, p50, p95, p99 = results[key]
        lines.append(b"%b:00:00Z %b %d %d %d %d %d %d\n"
                     % (h, e, n, p50, p95, p99, errs, errs * 1_000_000 // n))
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_NAME), "wb") as f:
        f.write(b"".join(lines))


# --------------------------------------------------------------------------- #
# reference algorithm (fallback, byte-for-byte the original behaviour)        #
# --------------------------------------------------------------------------- #

def _percentile(sorted_values, p):
    n = len(sorted_values)
    rank = (p * n + 99) // 100
    return sorted_values[rank - 1]


def reference_main(data_dir, out_dir):
    from datetime import datetime
    latencies = {}
    errors = {}
    with open(os.path.join(data_dir, INPUT_NAME), encoding="utf-8") as f:
        for line in f:
            ts, method, endpoint, status, latency_us = line.rstrip("\n").split(" ")
            when = datetime.strptime(ts, TIMESTAMP_FORMAT)
            hour = when.replace(minute=0, second=0, microsecond=0)
            key = (hour, endpoint)
            if key not in latencies:
                latencies[key] = []
                errors[key] = 0
            latencies[key].append(int(latency_us))
            if 500 <= int(status) <= 599:
                errors[key] += 1
    lines = []
    for key in sorted(latencies):
        hour, endpoint = key
        values = sorted(latencies[key])
        count = len(values)
        errs = errors[key]
        ppm = errs * 1_000_000 // count
        lines.append(
            f"{hour.strftime('%Y-%m-%dT%H:00:00Z')} {endpoint} {count} "
            f"{_percentile(values, 50)} {_percentile(values, 95)} {_percentile(values, 99)} "
            f"{errs} {ppm}\n"
        )
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_NAME), "w", encoding="utf-8", newline="\n") as f:
        f.write("".join(lines))


def main(data_dir, out_dir):
    gc.disable()
    try:
        fast_main(data_dir, out_dir)
    except Fallback:                     # input is not what the fast path assumes
        reference_main(data_dir, out_dir)
    except Exception:                    # fork/OOM/etc.: retry without processes first
        try:
            fast_main(data_dir, out_dir, single=True)
        except Exception:
            reference_main(data_dir, out_dir)


def _parse_args(argv):
    data, out = "/data", "/out"
    it = iter(argv)
    for a in it:
        if a in ("--data", "--out"):
            try:
                v = next(it)
            except StopIteration:
                sys.exit("error: argument %s: expected one argument" % a)
            if a == "--data":
                data = v
            else:
                out = v
        elif a.startswith("--data="):
            data = a[7:]
        elif a.startswith("--out="):
            out = a[6:]
        elif a in ("-h", "--help"):
            print("usage: a_ingest_fast.py [--data DATA] [--out OUT]")
            sys.exit(0)
        else:
            sys.exit("error: unrecognized argument: %s" % a)
    return data, out


if __name__ == "__main__":
    main(*_parse_args(sys.argv[1:]))
    sys.exit(0)
