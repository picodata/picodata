# Тестовый портал документации продукта Picodata

Здесь живёт проект Mkdocs, с помощью которого генерируется статичный сайт документации для Picodata. Содержимое репозитория попадает на сайт [docs.binary.picodata.io/picodata](docs.binary.picodata.io/picodata).

## Установка зависимостей
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Cборка проекта
```
mkdocs build -d site
```

## Локальный запуск
```
mkdocs serve
```

По умолчанию сайт будет доступен по адресу [http://127.0.0.1:8000](http://127.0.0.1:8000/).

Для выхода из временного окружения Python нажмите _Ctrl+D_.

## Сборка в Docker

В данном репозитории имееется [Dockerfile](docker/static/Dockerfile), с помощью которого можно собрать и запустить сайт внутри контейнера. Я использую следующие команды:
```
docker build -f docker/static/Dockerfile -t test-project-mkdocs --no-cache .
docker run -p 8080:8080 test-project-mkdocs:latest
```

<!-- ## Загрузка в Heroku
Я сделал небольшую обёртку в PHP для того чтобы статическую версию сайта можно было деплоить в Heroku. После генерации сайта следует запустить скрипт `./phpize.sh`


#### Настройка и деплой в первый раз[^1]:
```
cd site 
heroku create
heroku config:set NPM_CONFIG_PRODUCTION=false
heroku config:set HOST=0.0.0.0
heroku config:set NPM_CONFIG_PRODUCTION=false -a peaceful-brook-74799
heroku config:set HOST=0.0.0.0 -a peaceful-brook-74799
heroku config:set NODE_ENV=production -a peaceful-brook-74799
git init
git add .
git commit -m "First Heroku commit"
git remote add origin https://git.heroku.com/peaceful-brook-74799.git
git push -u origin master
```
[^1]:Примечание: имя приложения будет отличаться (см. `heroku apps`) -->

