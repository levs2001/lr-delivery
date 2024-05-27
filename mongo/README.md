## Поднятие кластера
```shell
docker-compose up
```


## Заливка данных
Запусти ``upload.py``


## Просмотр таблиц
По желанию можешь подключиться к монге и проверить, что все залилось.

```shell
docker exec -it 1.mongo.lr sh -c "mongo --port 30001"
```

```
use restaurant_db
db.clients.find()
db.restaurants.find()
db.orders.find()
```


## Смена схемы
Если хочешь поменять схему:
* Дропни нужную тебе таблицу ``db.clients.drop()``
* Поменяй соответствующий json файл, монга подхватит новую схему сама 