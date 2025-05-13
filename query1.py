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
            'cleaned_price': {
                '$toInt': {
                    '$arrayElemAt': [
                        {
                            '$split': [
                                {
                                    '$replaceAll': {
                                        'input': {
                                            '$toString': '$price'
                                        }, 
                                        'find': ',', 
                                        'replacement': ''
                                    }
                                }, '.'
                            ]
                        }, 0
                    ]
                }
            }
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
            }
        }
    }, {
        '$group': {
            '_id': {
                'neighborhood': '$neighbourhood_cleansed', 
                'property_size': '$property_size'
            }, 
            'max_price': {
                '$max': '$cleaned_price'
            }, 
            'min_price': {
                '$min': '$cleaned_price'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$addFields': {
            'price_range': {
                '$subtract': [
                    '$max_price', '$min_price'
                ]
            }
        }
    }, {
        '$addFields': {
            'price_elasticity': {
                '$divide': [
                    '$price_range', '$count'
                ]
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'neighborhood': '$_id.neighborhood', 
            'property_size': '$_id.property_size', 
            'max_price': 1, 
            'min_price': 1, 
            'price_elasticity': 1
        }
    }
]


# Execute aggregation pipeline
# result = list(collection.aggregate(pipeline))
data = list(collection.aggregate(pipeline))


# Get the neighborhood input from the user
target_neighborhood = input("Enter the neighborhood(Ex:Central City) you want to plot: ")

neighborhood_data = [d for d in data if d['neighborhood'] == target_neighborhood]
# neighborhood_data = [d for d in data if d['neighborhood'] == 'Central City']

# Create lists to store property sizes, max prices, and min prices
property_sizes = []
max_prices = []
min_prices = []
price_elasticity = []

# Iterate over filtered data to extract property sizes, max prices, and min prices
for entry in neighborhood_data:
    property_sizes.append(entry['property_size'])
    max_prices.append(entry['max_price'])
    min_prices.append(entry['min_price'])
    price_elasticity.append(entry['price_elasticity'])

# Plot the graph
plt.figure(figsize=(6, 6))

# Plot max prices with an arrow up symbol
# plt.scatter(property_sizes, max_prices, color='blue', marker='^', label='Max Price', s=35)  

# Plot min prices with an arrow down symbol
# plt.scatter(property_sizes, min_prices, color='red', marker='v', label='Min Price', s=35)

# plt.scatter(property_sizes,price_elasticity , color='green', marker='o', label='Price Elasticity', s=35)
plt.bar(property_sizes,price_elasticity , color='green', label='Price Elasticity')
# Highlight max and min points with different colors
for i in range(len(property_sizes)):
    # plt.text(property_sizes[i], max_prices[i], f'{max_prices[i]}', ha='right', va='bottom', color='blue')
    # plt.text(property_sizes[i], min_prices[i], f'{min_prices[i]}', ha='right', va='bottom', color='red')
    plt.text(property_sizes[i], price_elasticity[i], f'{price_elasticity[i]}', ha='right', va='bottom', color='green')

# Add labels and title
plt.xlabel('Property Size')
plt.ylabel('Price Elasticity')
plt.title(f'Price Elasticity vs Property Size for {target_neighborhood}')
plt.legend()

# Show plot
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# Write output to JSON file with readable formatting
with open('output1.json', 'w') as f:
    json.dump(data, f, indent=4, default=str)

client.close()