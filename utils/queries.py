# Query 1: Number of Electric Vehicles per Model Year along with the Electric Vehicle Type
def query_vehicles_by_year_and_type(collection):
    pipeline = [
        {
            "$group": {
                "_id": { "modelYear": "$Model Year", "type": "$Electric Vehicle Type" },
                "count": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "_id": 0,
                "modelYear": "$_id.modelYear",
                "electricVehicleType": "$_id.type",
                "count": 1
            }
        },
        { "$sort": { "modelYear": 1, "electricVehicleType": 1 } }
    ]
    return list(collection.aggregate(pipeline))

# Query 2: Number of Electric Vehicles Registered per County per Model Year
def query_vehicles_by_county_and_year(collection):
    pipeline = [
        {
            "$group": {
                "_id": { "county": "$County", "modelYear": "$Model Year" },
                "count": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "_id": 0,
                "county": "$_id.county",
                "modelYear": "$_id.modelYear",
                "count": 1
            }
        },
        { "$sort": { "county": 1, "modelYear": 1 } }
    ]
    return list(collection.aggregate(pipeline))

# Query 3: Number of Electric Vehicles per "Make" per Model Year
def query_vehicles_by_make_and_year(collection):
    pipeline = [
        {
            "$group": {
                "_id": { "make": "$Make", "modelYear": "$Model Year" },
                "count": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "_id": 0,
                "make": "$_id.make",
                "modelYear": "$_id.modelYear",
                "count": 1
            }
        },
        { "$sort": { "make": 1, "modelYear": 1 } }
    ]
    return list(collection.aggregate(pipeline))

