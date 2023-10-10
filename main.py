import json
import faiss
import pymongo
import os
import numpy as np
from bson.objectid import ObjectId

conn = pymongo.MongoClient(host=os.environ.get('MONGO_HOST'), port=int(os.environ.get('MONGO_PORT')), username=os.environ.get('MONGO_USERNAME'), password=os.environ.get('MONGO_PASSWORD'))
db = conn[os.environ.get('MONGO_DBNAME')]

def lambda_handler(event, context):
    try:
        queries = event['queryStringParameters']
        id = ObjectId(queries['id'])
        latitude = float(queries['lat'])
        longitude = float(queries['lon'])
        max_distance = int(queries['dist'])
        size = int(queries['size'])

        stores_id_in_location = list(db.stores.aggregate([
            {
                '$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': [longitude, latitude]
                    },
                    'distanceField': 'distance',
                    'maxDistance': max_distance,
                    'spherical': True
                }
            },
            {
                '$project': {
                    '_id': 1
                }
            }
        ]))

        stores_id_in_location = [str(store_id['_id']) for store_id in stores_id_in_location]
        cake_documents = list(db.cakes.find({'owner_store_id': {"$in": stores_id_in_location}}))

        if len(cake_documents) == 0:
            return {
                "statusCode": 204,
                "headers": {
                    "Content-Type": "application/json; charset=utf-8"
                }
            }

        vit_vectors = np.array([cake_document['vit'] for cake_document in cake_documents]).astype('float32')
        vit_index = faiss.IndexFlatL2(1000)
        vit_index.add(vit_vectors)
        
        distances, indices = vit_index.search(np.array([db.cakes.find_one({"_id": id})['vit']]).astype('float32'), 500)

        result = []

        set_of_store_id = set()
        for i, faiss_index in enumerate(indices[0]):
            if len(result) == size:
                break
            if str(cake_documents[faiss_index]['owner_store_id']) in set_of_store_id:
                continue

            result.append(
                  {
                    "id": str(cake_documents[faiss_index]['_id']),
                    "image": cake_documents[faiss_index]['image'],
                    "owner_store_id": str(cake_documents[faiss_index]['owner_store_id']),
                    "createdAt": str(cake_documents[faiss_index]['createdAt']),
                    "updatedAt": str(cake_documents[faiss_index]['updatedAt']),
                    "score": float(distances[0][i])
                  }
            )
            set_of_store_id.add(str(cake_documents[faiss_index]['owner_store_id']))

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            },
            "body": json.dumps({
                "result": result
            })
        }

    except Exception as e:
        print(e)
        return {
            "statusCode": 400,
        }