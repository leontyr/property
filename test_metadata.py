import json, time, textwrap, sys
sys.path.insert(0, '/Users/bytedance/Documents/projects/vibe/propertyclaude')
from extract_metadata import extract_property_metadata, metadata_to_fields

with open('output/properties.json') as f:
    props = json.load(f)

candidates = [p for p in props if len(p.get('description', '')) > 200]
picked = []
for keyword in ['Victorian', 'south-facing', 'Thames', 'garage', 'cul-de-sac']:
    for p in candidates:
        if keyword.lower() in p.get('description', '').lower() and p not in picked:
            picked.append(p)
            break

print('=' * 72)
total = 0
for i, p in enumerate(picked, 1):
    desc = p.get('description', '')
    print(f'\n#{i}  {p["address"]}')
    print(f'    {textwrap.shorten(desc, 200)}')
    t0 = time.time()
    meta = extract_property_metadata(desc)
    secs = time.time() - t0
    total += secs
    f = metadata_to_fields(meta)
    print(f'  ⏱  {secs:.1f}s')
    print(f'  🌿 garden_facing  : {f["garden_facing"]}')
    print(f'  🌳 outdoor_space  : {f["outdoor_space"]}')
    print(f'  🏗  dev           : types={f["dev_types"]}  planning={f["dev_planning"]}  unmod={f["dev_unmodernized"]}')
    print(f'  🚗 parking        : {f["parking_type"]}  spaces={f["parking_spaces"]}  ev={f["parking_ev"]}')
    print(f'  🏛  period_feat   : {f["period_features"]}')
    print(f'  🪟 double_glazing : {f["double_glazing"]}')
    print(f'  🤫 quiet_rating   : {f["quiet_rating"]}')
    print(f'  🌊 river_prox     : {f["river_proximity"]}')

avg = total / len(picked)
print(f'\n{"=" * 72}')
print(f'Avg: {avg:.1f}s/property  |  Est for 1360: {avg*1360/60:.0f} min ({avg*1360/3600:.1f} hr)')
