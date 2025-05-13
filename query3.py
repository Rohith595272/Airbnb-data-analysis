from pymongo import MongoClient
import json
import matplotlib.pyplot as plt


# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['DB']
collection = db['project2']

# Aggregation pipeline
pipeline = [
    {
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
            'occupancy_rate': {
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
        '$lookup': {
            'from': 'project2', 
            'let': {
                'host_id': '$host_id'
            }, 
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$host_id', '$$host_id'
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
                        ], 
                        'host_response_time': {
                            '$in': [
                                'a few days or more', 'within a day', 'within a few hours', 'within an hour'
                            ]
                        }, 
                        'host_response_rate': {
                            '$type': 'string'
                        }, 
                        'host_acceptance_rate': {
                            '$type': 'string'
                        }, 
                        'host_identity_verified': {
                            '$type': 'bool'
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$host_id', 
                        'avg_response_rate': {
                            '$avg': {
                                '$toDouble': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$host_response_rate', '%'
                                            ]
                                        }, 0
                                    ]
                                }
                            }
                        }, 
                        'avg_acceptance_rate': {
                            '$avg': {
                                '$toDouble': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$host_acceptance_rate', '%'
                                            ]
                                        }, 0
                                    ]
                                }
                            }
                        }, 
                        'num_listings': {
                            '$sum': 1
                        }, 
                        'identity_verified': {
                            '$first': '$host_identity_verified'
                        }, 
                        'response_time': {
                            '$first': '$host_response_time'
                        }
                    }
                }, {
                    '$addFields': {
                        'response_time_score': {
                            '$switch': {
                                'branches': [
                                    {
                                        'case': {
                                            '$eq': [
                                                '$response_time', 'a few days or more'
                                            ]
                                        }, 
                                        'then': 1.5
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$response_time', 'within a day'
                                            ]
                                        }, 
                                        'then': 3
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$response_time', 'within a few hours'
                                            ]
                                        }, 
                                        'then': 4
                                    }, {
                                        'case': {
                                            '$eq': [
                                                '$response_time', 'within an hour'
                                            ]
                                        }, 
                                        'then': 5
                                    }
                                ], 
                                'default': 0
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'normalized_response_rate': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$avg_response_rate', 0
                                    ]
                                }, {
                                    '$subtract': [
                                        100, 0
                                    ]
                                }
                            ]
                        }, 
                        'normalized_acceptance_rate': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$avg_acceptance_rate', 0
                                    ]
                                }, {
                                    '$subtract': [
                                        100, 0
                                    ]
                                }
                            ]
                        }, 
                        'normalized_num_listings': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$num_listings', 0
                                    ]
                                }, {
                                    '$subtract': [
                                        100, 0
                                    ]
                                }
                            ]
                        }, 
                        'normalized_identity_verified': {
                            '$cond': {
                                'if': '$identity_verified', 
                                'then': 1, 
                                'else': 0
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'host_experience_score': {
                            '$avg': [
                                '$normalized_response_rate', '$normalized_acceptance_rate', '$normalized_num_listings', '$normalized_identity_verified', '$response_time_score'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'host_id': '$_id', 
                        'host_experience_score': 1
                    }
                }, {
                    '$sort': {
                        'host_experience_score': -1
                    }
                }
            ], 
            'as': 'host_info'
        }
    }, {
        '$addFields': {
            'host_experience_score': {
                '$arrayElemAt': [
                    '$host_info.host_experience_score', 0
                ]
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
        '$project': {
            '_id': 1, 
            'name': 1, 
            'property_size': 1, 
            'host_experience_score': 1, 
            'occupancy_rate': 1, 
            'availability_365': 1
        }
    }
]

result = list(collection.aggregate(pipeline))

# Convert ObjectId to string in the result
for doc in result:
    doc['_id'] = str(doc['_id'])


# Defining property sizes order
property_sizes_order = ['VeryLarge', 'Large', 'Medium', 'Small', 'VerySmall']


palette = plt.cm.hsv([i/float(len(property_sizes_order)) for i in range(len(property_sizes_order))])


filtered_result = [doc for doc in result if 'host_experience_score' in doc]


def filter_by_property_size(docs, size):
    return [doc for doc in docs if doc.get('property_size') == size]


fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 12))


for property_size, color in zip(property_sizes_order, palette):
    
    idx = property_sizes_order.index(property_size)
    row = idx // 2
    col = idx % 2
    ax = axes[row, col]
    
   
    filtered_docs = filter_by_property_size(filtered_result, property_size)
    
    
    host_exp_scores = [doc['host_experience_score'] for doc in filtered_docs]
    occupancy_rates = [doc['occupancy_rate'] for doc in filtered_docs]
    
    
    ax.scatter(host_exp_scores, occupancy_rates, color=color, s=5)
    
    ax.set_xlabel('Host Experience Score')
    ax.set_ylabel('Occupancy Rate')
    ax.set_title(f'Occupancy Rate vs Host Experience Score for {property_size} Properties')
    
    ax.grid(True)


for idx in range(len(property_sizes_order), len(axes.flatten())):
    axes.flatten()[idx].remove()

plt.tight_layout()
plt.show()

# Write output to JSON file with readable formatting
with open('output3.json', 'w') as f:
    json.dump(result, f, indent=4, default=str)

client.close()