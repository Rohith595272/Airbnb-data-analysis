from pymongo import MongoClient
import json
import matplotlib.pyplot as plt

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['DB']
collection = db['project2']


pipeline = [
    {
        '$addFields': {
            'amenity_score': {
                '$size': {
                    '$setIntersection': [
                        '$amenities', [
                            'Free street parking', 'Shower gel', 'Body soap', 'Essentials', 'Oven', 'Dedicated workspace', 'Heating', 'Conditioner', 'Backyard', 'Patio or balcony', 'Kitchen', 'First aid kit', 'Keypad', 'Iron', 'Extra pillows and blankets', 'Baking sheet', 'Cable TV', 'Fire extinguisher', 'Lockbox', 'Long term stays allowed', 'Elevator', 'Outdoor furniture', 'TV with standard cable', 'Hot water', 'Breakfast', 'High chair', 'Security cameras on property', 'Shampoo', 'Smart lock', 'Pack ’n play/Travel crib', 'Bed linens', 'Dishes and silverware', 'Luggage dropoff allowed', 'Lock on bedroom door', 'Stove', 'BBQ grill', 'Cooking basics', 'Hair dryer', 'Bathtub', 'Air conditioning', 'Free parking on premises', 'Room-darkening shades', 'Washer', 'Wifi', 'Refrigerator', 'Gym', 'Dining table', 'Toaster', 'Wine glasses', 'Microwave', 'Private entrance', 'Smoke alarm', 'Hangers', 'Dishwasher', 'TV', 'Ceiling fan', 'Cleaning products', 'Carbon monoxide alarm', 'Single level home', 'Freezer', 'Coffee maker', 'Dryer', 'Paid parking off premises'
                        ]
                    ]
                }
            }, 
            'matched_amenities': {
                '$setIntersection': [
                    '$amenities', [
                        'Free street parking', 'Shower gel', 'Body soap', 'Essentials', 'Oven', 'Dedicated workspace', 'Heating', 'Conditioner', 'Backyard', 'Patio or balcony', 'Kitchen', 'First aid kit', 'Keypad', 'Iron', 'Extra pillows and blankets', 'Baking sheet', 'Cable TV', 'Fire extinguisher', 'Lockbox', 'Long term stays allowed', 'Elevator', 'Outdoor furniture', 'TV with standard cable', 'Hot water', 'Breakfast', 'High chair', 'Security cameras on property', 'Shampoo', 'Smart lock', 'Pack ’n play/Travel crib', 'Bed linens', 'Dishes and silverware', 'Luggage dropoff allowed', 'Lock on bedroom door', 'Stove', 'BBQ grill', 'Cooking basics', 'Hair dryer', 'Bathtub', 'Air conditioning', 'Free parking on premises', 'Room-darkening shades', 'Washer', 'Wifi', 'Refrigerator', 'Gym', 'Dining table', 'Toaster', 'Wine glasses', 'Microwave', 'Private entrance', 'Smoke alarm', 'Hangers', 'Dishwasher', 'TV', 'Ceiling fan', 'Cleaning products', 'Carbon monoxide alarm', 'Single level home', 'Freezer', 'Coffee maker', 'Dryer', 'Paid parking off premises'
                    ]
                ]
            }
        }
    }, {
        '$match': {
            '$or': [
                {
                    'availability_30': {
                        '$gt': 0
                    }
                }, {
                    'availability_60': {
                        '$gt': 0
                    }
                }, {
                    'availability_90': {
                        '$gt': 0
                    }
                }, {
                    'availability_365': {
                        '$gt': 0
                    }
                }
            ]
        }
    }, {
        '$addFields': {
            'property_size': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$in': [
                                    '$property_type', [
                                        'Boat', 'Camper/RV', 'Private room in houseboat', 'Private room in tiny house', 'Room in aparthotel', 'Room in hostel', 'Room in hotel', 'Shared room in tiny house'
                                    ]
                                ]
                            }, 
                            'then': 'VerySmall'
                        }, {
                            'case': {
                                '$in': [
                                    '$property_type', [
                                        'Barn', 'Houseboat', 'Private room in bed and breakfast', 'Private room in bungalow', 'Private room in condominium (condo)', 'Private room in cottage', 'Private room in earth house', 'Private room in farm stay', 'Private room in guest suite', 'Private room in guesthouse', 'Private room in hostel', 'Private room in loft', 'Private room in rental unit', 'Private room in residential home', 'Private room in resort', 'Private room in serviced apartment', 'Private room in townhouse', 'Room in bed and breakfast', 'Shared room in condominium (condo)', 'Shared room in cottage', 'Shared room in guesthouse', 'Shared room in hostel', 'Shared room in rental unit', 'Shared room in residential home'
                                    ]
                                ]
                            }, 
                            'then': 'Small'
                        }, {
                            'case': {
                                '$in': [
                                    '$property_type', [
                                        'Entire guest suite', 'Entire home/apt', 'Entire loft', 'Entire rental unit', 'Room in aparthotel', 'Room in boutique hotel', 'Room in hotel', 'Room in resort', 'Room in serviced apartment', 'Tiny house'
                                    ]
                                ]
                            }, 
                            'then': 'Medium'
                        }, {
                            'case': {
                                '$in': [
                                    '$property_type', [
                                        'Entire cottage', 'Entire guesthouse', 'Entire residential home', 'Entire serviced apartment', 'Entire townhouse', 'Farm stay'
                                    ]
                                ]
                            }, 
                            'then': 'Large'
                        }, {
                            'case': {
                                '$in': [
                                    '$property_type', [
                                        'Castle', 'Entire bungalow', 'Entire condominium (condo)', 'Entire place', 'Entire resort', 'Entire villa', 'Floor'
                                    ]
                                ]
                            }, 
                            'then': 'VeryLarge'
                        }
                    ], 
                    'default': 'Unknown'
                }
            }, 
            'customer_prefer': {
                '$multiply': [
                    {
                        '$subtract': [
                            365, '$availability_365'
                        ]
                    }, {
                        '$divide': [
                            100, 365
                        ]
                    }
                ]
            }
        }
    }, {
        '$group': {
            '_id': '$neighbourhood_cleansed', 
            'property_size': {
                '$first': '$property_size'
            }, 
            'avg_occupancy_rate': {
                '$avg': '$customer_prefer'
            }, 
            'avg_amenity_score': {
                '$avg': '$amenity_score'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'neighbourhood_cleansed': '$_id', 
            'property_size': 1, 
            'customer_prefer': '$avg_occupancy_rate', 
            'avg_amenity_score': 1
        }
    }
]


# Execute aggregation pipeline
# result = list(collection.aggregate(pipeline))
data = list(collection.aggregate(pipeline))


#Function to format the data as a table
def format_table(data):
    
    headers = ["Neighbourhood", "Property Size", "Amenity Score", "Customer Preference"]
    table = [headers]
    for entry in data:
        row = [entry["neighbourhood_cleansed"], entry["property_size"], entry["avg_amenity_score"], entry["customer_prefer"]]
        table.append(row)
    return table

# Writing the table to a CSV file
with open('output2_table.csv', 'w') as f:
    
    table_data = format_table(data)
    for row in table_data:
        f.write(','.join(map(str, row)) + '\n')

print("Table has been saved to: output_table.csv")

# Write output to JSON file with readable formatting
with open('output2.json', 'w') as f:
    json.dump(data, f, indent=4, default=str)

client.close()