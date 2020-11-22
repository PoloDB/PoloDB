
# PoloDB for Python

## Install

```
pip install polodb
```

## Compile yourself

cd to `pypolodb` directory

```shell script
python3 setup.py build
```

## Open a database

Remember to close it.

```python
import polodb

try:
    db = polodb.Database('test.db')
finally:
    db.close()
    
```

## Insert a row to the database

```python
collection = db.collection('students')

collection.insert({
    'name': 'Vincent Chan',
    'age': 14,
})
```

## Find

```python
collection = db.collection('students')

result = collection.find({
    'name': 'Vincent Chan'
})

print(result)
```

## Update

```python
collection.update({
    'name': 'Vincent Chan'
}, {
    '$inc': {
        'age': 1,
    },
})
```

## Delete

```python
collection.delete({
    'name': 'Vincent Chan',
})
```

delete all

```python
collection.deleteAll()
```

## Transactions

```python
db.startTransaction()
db.commit()
db.rollback()
```
