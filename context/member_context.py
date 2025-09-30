from pymongo import MongoClient



mongo_host = "mongodb://root:f41a7bc0-f8da-478d-b7fb-672ff53c8d7a@localhost:27017/?authSource=admin&directConnection=true&readPreference=primaryPreferred"
client = MongoClient(mongo_host)

db = client['OmniService']
collection = db['wn_omni_members']




def find_one (filter : dict) : 
    member = collection.find_one(filter)

    if member is None :
        return member
    
    return dict(member)
