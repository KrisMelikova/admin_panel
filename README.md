## API для кинотеатра 
### возвращает список фильмов в формате, описанном в openapi-файле, и позволяющий получить информацию об одном фильме
### также осуществляется загрузка данных в Postgres и перенос данных в Elasticsearch

____________________________________________________________________________
Как запустить проект и проверить его работу:
____________________________________________________________________________
Запуск приложения с docker compose:
```
docker-compose up
```

Приложение будет доступно по адресу:
```
http://127.0.0.1:80
```

Админка:
```
http://127.0.0.1:80/admin
```

____________________________________________________________________________
Часть, которая будет актуальна при разработке и отладке:
____________________________________________________________________________
Активация виртуального окружения
```
source venv/bin/activate
```

Заполнение базы данными (в ручном режиме):
```
1) Меняем в .env значение переменной DB_HOST на 'localhost' (текущее значение: DB_HOST='postgres')
2) cd sqlite_to_postgres
3) python load_data.py
```

Генерация superuser, чтобы войти в админку:
```
1) docker ps
2) docker exec -it <django app container id> bash
3) cd movies_app
4) python manage.py createsuperuser
```

Установка зависимостей
```
pip3 install -r requirements.txt
or
pip3 install -r requirements-dev.txt
```

Запуск отладочного сервера:
```
python manage.py runserver <port>
python manage.py migrate
python3 load_data.py (см. sqlite_to_postgres папку)
```

Вход в интерактивный интерпретатор
```
python manage.py shell
```
Docker:
```
docker build -t django_api .
docker run -p 8000:8000 --rm --name django django_api
docker container prune
```

Для отладки c docker compose могут пригодиться
```
docker-compose down
docker-compose build
docker exec -it <container id> bash
```

Посмотреть все индексы в Elasticsearch
```
http://127.0.0.1:9200/_cat/indices\?v
```
Посмотреть все записи в индексе
```
http://localhost:9200/<index>/_search?pretty=true&q=*:*
```