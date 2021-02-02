import polodb
import os.path

DB_PATH = '/tmp/test-py.db'

def test_open():
  if os.path.exists(DB_PATH):
    print('database exist, remove')
    os.remove(DB_PATH)
  db = polodb.Database(DB_PATH)
  db.close()

def test_create_collection():
  db = polodb.Database(DB_PATH)
  try:
    collection = db.createCollection('test')
    collection.insert({
      'name': 'Vincent Chan',
      'age': 14,
    })
    result = collection.find({
      'name': 'Vincent Chan',
      'age': 14,
    })
    assert len(result) == 1
    assert result[0]['name'] == 'Vincent Chan'
    assert result[0]['age'] == 14
  finally:
    db.close()
  
  # open again
  db = polodb.Database(DB_PATH)
  try :
    collection = db.collection('test')
    result = collection.find({
      'name': 'Vincent Chan',
      'age': 14,
    })
    print(len(result))
    assert len(result) == 1
    assert result[0]['name'] == 'Vincent Chan'
    assert result[0]['age'] == 14
  finally:
    db.close()
