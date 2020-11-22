
# PoloDB 的 Python 版本

## 安装

```
pip install polodb
```

## 自己编译

进入到 pypolodb 目录下

```shell script
python3 setup.py build
```

## 打开一个数据库

避免数据丢失，记得关闭数据库。

```python
import polodb

try:
    db = polodb.Database('test.db')
finally:
    db.close()
    
```

## 往数据集插入一条数据

```python
collection = db.collection('students')

collection.insert({
    'name': 'Vincent Chan',
    'age': 14,
})
```

## 查找数据

```python
collection = db.collection('students')

result = collection.find({
    'name': 'Vincent Chan'
})

print(result)
```

## 更新数据

```python
collection.update({
    'name': 'Vincent Chan'
}, {
    '$inc': {
        'age': 1,
    },
})
```

## 删除数据

```python
collection.delete({
    'name': 'Vincent Chan',
})
```

删除数据集里面的所有数据。

```python
collection.deleteAll()
```

## 事务

```python
db.startTransaction()
db.commit()
db.rollback()
```
