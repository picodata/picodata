# Документация Picodata

В репозитории расположены исходные файлы проекта [MkDocs](https://www.mkdocs.org/), с помощью которого генерируется статический сайт документации Picodata — [https://docs.picodata.io/picodata/](https://docs.picodata.io/picodata/)

# Содержание

* [Тестирование документации Picodata](#тестирование-документации-picodata)
    * [Установка Pipenv](#установка-pipenv)
    * [Клонирование репозитория](#клонирование-репозитория)
    * [Установка зависимостей](#установка-зависимостей)
    * [Запуск локального сервера MkDocs](#запуск-локального-сервера-mkdocs)
    * [Сборка документации](#сборка-документации)
    * [Линтинг скриптов Python](#линтинг-скриптов-python)
    * [Форматирование скриптов Python](#форматирование-скриптов-python)
    * [Активация виртуального окружения Pipenv](#активация-виртуального-окружения-pipenv)
    * [Добавление зависимостей](#добавление-зависимостей)
* [Запуск документации Picodata в Docker](#запуск-документации-picodata-в-docker)

# Тестирование документации Picodata

## Установка Pipenv

Откройте терминал, введите команду:

``` shell
pip install pipenv
```

Для установки Pipenv потребуются Python не ниже версии 3.9 и актуальный `pip`

Подробнее — [Pipenv Installation](https://pipenv.pypa.io/en/latest/installation.html)

## Клонирование репозитория

Откройте терминал, последовательно введите команды:

``` shell
git clone --recurse-submodules https://git.picodata.io/core/picodata.git
cd picodata/docs
```

## Установка зависимостей

Введите команду:

``` shell
pipenv sync -d
```

С помощью этой команды будут установлены группы модулей `[packages]` и `[dev-packages]`, указанные в [Pipfile](Pipfile)

## Запуск локального сервера MkDocs

Введите команду:

``` shell
pipenv run serve
```

Локальный сайт документации Picodata будет доступен по адресу [http://127.0.0.1:8000](http://127.0.0.1:8000/)

Для остановки локального сервера MkDocs нажмите `Ctrl + Z`

## Сборка документации

Введите команду:

``` shell
pipenv run build
```

С помощью этой команды сайт документации будет собран [в «строгом» режиме](https://www.mkdocs.org/user-guide/cli/#mkdocs-build) — с флагом `-s` / `--strict`

Полученную сборку можно запустить, например, с помощью модуля [http.server](https://docs.python.org/3/library/http.server.html). Введите команду:

``` shell
python -m http.server -d site --bind 127.0.0.1
```

## Получение документации в формате PDF

Соберите веб-версию документации:

``` shell
pipenv run mkdocs build -sd build
```

Установите веб-браузер Chromium (необходим для генерации PDF-файла).

Перейдите в директорию `build` и выполните команду:

```shell
chromium-browser --headless --disable-gpu --no-sandbox --no-zygote --disable-software-rasterizer --disable-dev-shm-usage --no-pdf-header-footer --print-to-pdf=picodata_docs.pdf picodata_docs.html
```

Для добавления номеров страниц в колонтитуле:

Установите утилиты [pdfcpu](https://github.com/pdfcpu/pdfcpu) и pdfinfo (входит в состав [poppler-utils](https://github.com/elswork/poppler-utils)).

Выполните команду:

``` shell
pdfcpu stamp add -p 2-$(pdfinfo picodata_docs.pdf | awk '/^Pages:/ {print $2}') -mode text -- "%p  " "scale:.4 abs, pos:br, rot:0, margins:10" picodata_docs.pdf
```

## Линтинг скриптов Python

Введите команду:

``` shell
pipenv run lint
```

Хуки из папки `hooks` будет последовательно проверены с помощью следующих модулей:

* [`flake8`](https://github.com/pycqa/flake8/)
* [`black`](https://github.com/psf/black)
* [`mypy`](https://github.com/python/mypy)

## Форматирование скриптов Python

Введите команду:

``` shell
pipenv run fmt
```

Форматирование хуков из папки `hooks` будет скорректировано форматтером `black`

## Активация виртуального окружения Pipenv

Введите команду:

``` shell
pipenv shell
```

Активированное виртуальное окружение Pipenv позволит использовать напрямую команды MkDocs и остальных установленных модулей

Для выхода из виртуального окружения Pipenv введите `exit` или нажмите `Ctrl + D`

## Добавление зависимостей

Введите команду:

``` shell
pipenv update <package>
```

где `<package>` — название добавляемого модуля

Команда добавляет модуль в [Pipfile](Pipfile) и [Pipfile.lock](Pipfile.lock), затем устанавливает его в виртуальное окружение Pipenv

# Запуск документации Picodata в Docker

С помощью [Dockerfile](docker/static/Dockerfile) можно собрать сайт документации Picodata внутри образа [Docker](https://docs.docker.com/), затем запустить образ в контейнере

Создание образа `picodocs`:

``` shell
docker build -f docker/static/Dockerfile -t picodocs --no-cache .
```

Запуск образа `picodocs` в контейнере `picodocs`:

``` shell
docker run --name picodocs -p 127.0.0.1:8000:8000 picodocs
```

Запущенный в контейнере `picodocs` сайт документации Picodata будет доступен по адресу [http://127.0.0.1:8000](http://127.0.0.1:8000/)

Удаление контейнера `picodocs`:

``` shell
docker rm -f picodocs
```
