import json

def is_bool_str(v):
    return isinstance(v, str) and v.lower() in ("true", "false")

def is_int_str(v):
    if not isinstance(v, str):
        return False
    s = v.strip()
    if s == "":
        return False
    s2 = s[1:] if s[0] == '-' else s
    return s2.isdigit() and s2 != ""

def is_float_val(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return True
    if isinstance(v, str):
        try:
            float(v)
            return True
        except ValueError:
            return False
    return False

def is_float_noninteger(v):
    if isinstance(v, bool):
        return False
    if isinstance(v, float):
        return not v.is_integer()
    if isinstance(v, str):
        try:
            f = float(v)
            return not f.is_integer()
        except ValueError:
            return False
    return False

def infer_field_type(values):
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "string"
    if all(isinstance(v, bool) or is_bool_str(v) for v in non_null):
        return "boolean"
    def is_int(v):
        if isinstance(v, bool):
            return False
        if isinstance(v, int):
            return True
        return is_int_str(v)
    if all(is_int(v) for v in non_null):
        return "long"
    if all(is_float_val(v) for v in non_null) and any(is_float_noninteger(v) for v in non_null):
        return "double"
    return "string"

def field_values(rows, key):
    return [r.get(key) for r in rows if isinstance(r, dict)]

wl = json.load(open(".tmp_work/watchlist_raw.json"))["response"]
lt = json.load(open(".tmp_work/lookup_table_raw.json"))["data"]

print("=== watchlist top-level fields (n=%d) ===" % len(wl))
top_keys = set()
for r in wl:
    top_keys.update(r.keys())
top_keys.discard("entries")
for k in sorted(top_keys):
    vals = field_values(wl, k)
    nn = [v for v in vals if v is not None]
    print(f"{k:20s} -> {infer_field_type(vals):8s} nonnull={len(nn)}/{len(vals)} sample={nn[:3]}")

print()
print("=== watchlist entries fields ===")
all_entries = []
for r in wl:
    e = r.get("entries")
    if e:
        all_entries.extend(e)
print("total entries sampled:", len(all_entries))
entry_keys = set()
for e in all_entries:
    entry_keys.update(e.keys())
for k in sorted(entry_keys):
    vals = field_values(all_entries, k)
    nn = [v for v in vals if v is not None]
    print(f"{k:20s} -> {infer_field_type(vals):8s} nonnull={len(nn)}/{len(vals)} sample={nn[:3]}")

print()
print("=== lookup_table top-level fields (n=%d) ===" % len(lt))
top_keys2 = set()
for r in lt:
    top_keys2.update(r.keys())
top_keys2.discard("columnList")
for k in sorted(top_keys2):
    vals = field_values(lt, k)
    nn = [v for v in vals if v is not None]
    print(f"{k:20s} -> {infer_field_type(vals):8s} nonnull={len(nn)}/{len(vals)} sample={nn[:3]}")

print()
print("=== lookup_table columnList fields ===")
all_cols = []
for r in lt:
    c = r.get("columnList")
    if c:
        all_cols.extend(c)
print("total columns sampled:", len(all_cols))
col_keys = set()
for c in all_cols:
    col_keys.update(c.keys())
for k in sorted(col_keys):
    vals = field_values(all_cols, k)
    nn = [v for v in vals if v is not None]
    print(f"{k:20s} -> {infer_field_type(vals):8s} nonnull={len(nn)}/{len(vals)} sample={nn[:3]}")

# distinct type values for columnList.type, to double check safety
type_vals = set(c.get("type") for c in all_cols)
print("distinct columnList.type values:", type_vals)
