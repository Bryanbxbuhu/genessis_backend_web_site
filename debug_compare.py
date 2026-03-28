from storage import get_datastore

ds = get_datastore()

print("=== LONDON ===")
pharm_l = ds.get_curated_places("london", "pharmacy")
conv_l = ds.get_curated_places("london", "convenience")
print(f"Pharmacies: {len(pharm_l)}")
if pharm_l:
    for p in pharm_l[:2]:
        print(f"  - {p.get('name')}: website='{p.get('website', '')}'")
print(f"Convenience: {len(conv_l)}")
if conv_l:
    for c in conv_l[:2]:
        print(f"  - {c.get('name')}: website='{c.get('website', '')}'")

print("\n=== ROME ===")
pharm_r = ds.get_curated_places("rome", "pharmacy")
conv_r = ds.get_curated_places("rome", "convenience")
print(f"Pharmacies: {len(pharm_r)}")
if pharm_r:
    for p in pharm_r[:2]:
        print(f"  - {p.get('name')}: website='{p.get('website', '')}'")
print(f"Convenience: {len(conv_r)}")
if conv_r:
    for c in conv_r[:2]:
        print(f"  - {c.get('name')}: website='{c.get('website', '')}'")
        
print("\n=== ROME CONTEXT ===")
ctx = ds.get_city_context("rome")
if ctx:
    c = ctx.context if isinstance(ctx.context, dict) else {}
    print(f"Pharmacies in context: {len(c.get('pharmacies', []))}")
    print(f"Convenience stores in context: {len(c.get('convenience_stores', []))}")
