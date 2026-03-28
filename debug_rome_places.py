from storage import get_datastore

ds = get_datastore()

pharm = ds.get_curated_places('rome', 'pharmacy')
conv = ds.get_curated_places('rome', 'convenience')
hosp = ds.get_curated_places('rome', 'hospital')

print(f'Pharmacies: {len(pharm)}')
print(f'Convenience: {len(conv)}')
print(f'Hospitals: {len(hosp)}')

# Show some details
if pharm:
    print(f"\nPharmacies ({len(pharm)}):")
    for p in pharm[:2]:
        print(f"  - {p}")
else:
    print("\nNo pharmacies found - checking if Rome is in database...")

# Check context
ctx = ds.get_city_context('rome')
if ctx:
    print(f"\nCity context exists: {ctx.fetched_at}")
    if isinstance(ctx.context, dict):
        print(f"  - Hospitals in context: {len(ctx.context.get('hospitals', []))}")
        print(f"  - Pharmacies in context: {len(ctx.context.get('pharmacies', []))}")
        print(f"  - Convenience stores in context: {len(ctx.context.get('convenience_stores', []))}")
else:
    print("\nNo city context found for Rome")
