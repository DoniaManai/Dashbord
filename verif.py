import json
from datetime import datetime

def parse_iso(s):
    if not s: return None
    s = s.replace(' ', 'T')
    if s.endswith('Z'): s = s[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def get_interval(props):
    # begin/end
    b = parse_iso(props.get('begin')); e = parse_iso(props.get('end'))
    if b or e: return b, e
    # start/end
    b = parse_iso(props.get('start')); e = parse_iso(props.get('end'))
    if b or e: return b, e
    # timestamp
    ts = parse_iso(props.get('timestamp') or props.get('time') or props.get('datetime'))
    if ts: return ts, ts
    # date + hour
    d = props.get('date') or props.get('day'); h = props.get('hour') or props.get('heure') or props.get('h')
    if d and h is not None:
        try:
            h = int(h)
            return parse_iso(f"{d}T{h:02d}:00:00"), parse_iso(f"{d}T{h:02d}:59:59")
        except: pass
    # date only
    if d:
        return parse_iso(f"{d}T00:00:00"), parse_iso(f"{d}T23:59:59")
    return None, None

with open('traffic_agg.geojson','r',encoding='utf-8') as f:
    gj = json.load(f)

min_begin = None
max_end = None
for ft in gj.get('features', []):
    b, e = get_interval(ft.get('properties', {}) or {})
    if b and (min_begin is None or b < min_begin): min_begin = b
    if e and (max_end   is None or e > max_end):   max_end   = e

print('min_begin =', min_begin)
print('max_end   =', max_end)
