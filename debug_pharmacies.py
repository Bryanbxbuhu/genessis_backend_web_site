from storage import get_datastore
from report_loader import ReportDataLoader

ds = get_datastore()
loader = ReportDataLoader(datastore=ds)

# Test the _load_curated_places method directly
pharmacies_result = loader._load_curated_places('rome', 'pharmacy', target_country='IT')
convenience_result = loader._load_curated_places('rome', 'convenience', target_country='IT')

print(f"Pharmacies from _load_curated_places: {len(pharmacies_result)}")
if pharmacies_result:
    for p in pharmacies_result[:2]:
        print(f"  - {p.get('name')}")

print(f"\nConvenience stores from _load_curated_places: {len(convenience_result)}")
if convenience_result:
    for c in convenience_result[:2]:
        print(f"  - {c.get('name')}")

# Check if previous report exists
prev_report = ds.get_city_report('rome')
if prev_report:
    print(f"\nPrevious report exists")
    if hasattr(prev_report, 'report_data') and isinstance(prev_report.report_data, dict):
        data = prev_report.report_data
        print(f"  - Pharmacies in previous: {len(data.get('pharmacies', []))}")
        print(f"  - Convenience stores in previous: {len(data.get('convenience_stores', []))}")
        if isinstance(data.get('supplies_services'), dict):
            print(f"  - Supplies services pharmacies: {len(data['supplies_services'].get('pharmacies', []))}")
else:
    print("\nNo previous report found")
