from bson import ObjectId


def get_store_pipeline(longitude: float, latitude: float, distance: int, owner_store_id: str, size: int) -> list:
    store_pipeline = [
        {
            '$geoNear': {
                'near': {
                    'type': 'Point',
                    'coordinates': [longitude, latitude]
                },
                'distanceField': 'distance',
                'maxDistance': distance,
                'spherical': True
            }
        },
        {
            '$match': {
                '_id': {
                    '$ne': ObjectId(owner_store_id)
                }
            }
        },
        {
            "$addFields": {
                'string_id': {
                    "$toString": "$_id"
                }
            }
        },
        {
            "$lookup": {
                "from": 'cakes',
                "let": {'owner_store_id': {"$toString": '$_id'}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ['$owner_store_id', '$$owner_store_id']
                            }
                        }
                    },
                    {
                        "$limit": 1
                    }
                ],
                "as": 'first_cake'
            }
        },
        {
            "$match": {
                "first_cake": {
                    "$ne": []
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "first_cake": 1
            }
        },
        {
            "$limit": size
        }
    ]

    return store_pipeline

def get_cake_pipeline(stores_id_in_location: list, total_cake_documents: int, sorted_distance: list, size: int) -> list:
    cake_pipeline = [
        {
            '$match': {
                    '$and': [
                        {'is_delete': False},
                        {'owner_store_id': {'$in': stores_id_in_location}},
                        {'faiss_id': {'$lt': total_cake_documents}}
                    ]
            }
        },
        {
            '$project': {
                'vit': 0,
                'koclip': 0
            }
        },
        {
            '$addFields': {
                'score': {
                    '$arrayElemAt': [sorted_distance, '$faiss_id']
                }
            }
        },
        {
            '$sort': {
                'score': 1
            }
        },
        {
            '$group': {
                '_id': '$owner_store_id',
                'first_cake': {'$first': '$$ROOT'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'first_cake': 1
            }
        },
        {
            '$replaceRoot': {
                'newRoot': '$first_cake'
            }
        },
        {
            '$sort': {
                'score': 1
            }
        },
    ]

    return cake_pipeline