import json
import faiss
import pymongo
import os
import numpy as np
from bson.objectid import ObjectId
from .util.exceptions import StoreNoContentException, CakeNotFoundException
from .util.pipelines import get_cake_pipeline, get_store_pipeline

conn = pymongo.MongoClient(host=os.environ.get('MONGO_HOST'), port=int(os.environ.get('MONGO_PORT')), username=os.environ.get('MONGO_USERNAME'), password=os.environ.get('MONGO_PASSWORD'))
db = conn[os.environ.get('MONGO_DBNAME')]

vit_index = faiss.read_index(os.environ.get('INDEX_SAVE_PATH') + '/vit.index')
total_cake_documents = vit_index.ntotal

def to_json(bson: dict, score: float=None) -> dict:
    json = {}
    json['id'] = str(bson['_id'])
    json['image'] = bson['image']
    json['cursor'] = bson['cursor']
    json['createdAt'] = str(bson['createdAt'])
    json['updatedAt'] = str(bson['updatedAt'])
    json['faiss_id'] = bson['faiss_id']
    json['tag_ins'] = bson['tag_ins']
    json['owner_store_id'] = bson['owner_store_id']
    json['user_like_ids'] = bson['user_like_ids']
    json['score'] = score if score is not None else bson['score']

    return json

def get_similar_cakes_no_location(cake_document: dict, size: int, count_delete: int) -> list:
    distances, indices = vit_index.search(np.array([cake_document['vit']]).astype('float32'), total_cake_documents)

    distances_list = list(map(lambda x: x / 100, distances[0].tolist()))
    faiss_ids = indices[0].tolist()

    zip_data = list(zip(distances_list, faiss_ids))
    zip_data.sort(key=lambda x: x[1])
    sorted_distances = list(map(lambda x: x[0], zip_data))

    pipeline = [
		{
            '$match': {
                '$and': [
                    {'is_delete': False},
                    {'faiss_id': {'$in': faiss_ids[1:size + 1 + count_delete]}},
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
                    '$arrayElemAt': [sorted_distances, '$faiss_id']
                }
            }
		},
		{
            '$sort': {
                'score': 1
            }
		},
		{
			'$limit': size
		}
    ]

    cake_documents = list(db.cakes.aggregate(pipeline))
    cake_documents = list(map(lambda x: to_json(x), cake_documents))

    return cake_documents

def get_similar_cakes_with_location(cake_document: dict, size: int, latitude: float, longitude: float, distance: int) -> list:
    owner_store_id = cake_document['owner_store_id']

    store_pipeline = get_store_pipeline(longitude, latitude, distance, owner_store_id, size)

    stores_id_in_location = list(db.stores.aggregate(store_pipeline))
    stores_id_in_location = list(map(lambda x: str(x['_id']), stores_id_in_location))

    if stores_id_in_location == 0:
        raise StoreNoContentException()

    distances, indices = vit_index.search(np.array([cake_document['vit']]).astype('float32'), total_cake_documents)

    distances_list = distances[0].tolist()
    faiss_ids = indices[0].tolist()

    zip_data = list(zip(distances_list, faiss_ids))
    zip_data.sort(key=lambda x: x[1])
    sorted_distances = list(map(lambda x: x[0] / 100, zip_data))

    cake_pipeline = get_cake_pipeline(stores_id_in_location, total_cake_documents, sorted_distances, size)

    result = list(map(to_json, db.cakes.aggregate(cake_pipeline)))
    
    return result

def lambda_handler(event: dict, context: dict) -> dict:
    global vit_index
    global total_cake_documents
    if db.counters.find_one({'sequenceName': 'cakes'})['seq'] + 1 != total_cake_documents:
        vit_index = faiss.read_index(os.environ.get('INDEX_SAVE_PATH') + '/vit.index')
        total_cake_documents = vit_index.ntotal

    try:
        queries = event['queryStringParameters']
        cake_id = ObjectId(queries['id'])
        size = int(queries['size'])

        cake_document = db.cakes.find_one({"_id": cake_id})

        if cake_document is None:
            raise CakeNotFoundException()

        if (queries.get('lat') is None) or (queries.get('lon') is None) or (queries.get('dist') is None):
            count_delete = db.cakes.count_documents({'is_delete': True})
            result = get_similar_cakes_no_location(cake_document, size, count_delete)
        else:
            latitude = float(queries['lat'])
            longitude = float(queries['lon'])
            distance = int(queries['dist'])
            result = get_similar_cakes_with_location(cake_document, size, latitude, longitude, distance)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            },
            "body": json.dumps({
                "result": result
            })
        }
    except CakeNotFoundException as e:
        return {
            "statusCode": 404,
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            },
            "body": json.dumps({
                "message": "Cake not found"
            })
        }
    except StoreNoContentException as e:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            },
            "body": json.dumps({
                "message": "No store in location"
            })
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
        }