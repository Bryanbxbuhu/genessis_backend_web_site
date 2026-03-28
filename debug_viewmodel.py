from storage import get_datastore
from report_loader import ReportDataLoader
from coverage import ReportCoverage

ds = get_datastore()
loader = ReportDataLoader(datastore=ds)

# Load the full report data like the report generator does
vm = loader.load_report_data('rome', lookback_hours=48)

print("ReportViewModel contents:")
print(f"  Hospitals: {len(vm.hospitals)} items")
if vm.hospitals:
    for h in vm.hospitals[:2]:
        print(f"    - {h}")
        
print(f"  Pharmacies: {len(vm.pharmacies)} items")
if vm.pharmacies:
    for p in vm.pharmacies[:2]:
        print(f"    - {p.get('name')}: {p.get('website', 'no website')}")
        
print(f"  Convenience stores: {len(vm.convenience_stores)} items")
if vm.convenience_stores:
    for c in vm.convenience_stores[:2]:
        print(f"    - {c.get('name')}")
        
print(f"  Supermarkets: {len(vm.supermarkets)} items")
if vm.supermarkets:
    for s in vm.supermarkets[:2]:
        print(f"    - {s.get('name')}")
