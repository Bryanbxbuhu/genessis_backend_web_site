from storage import get_datastore

ds = get_datastore()

for city in ["paris", "barcelona", "madrid", "london", "rome"]:
    pharm = ds.get_curated_places(city, "pharmacy")
    conv = ds.get_curated_places(city, "convenience")
    hosp = ds.get_curated_places(city, "hospital")
    print(f"{city:12}: pharm={len(pharm):2}, conv={len(conv):2}, hosp={len(hosp):2}")
