# Пример работы с SQL Broadcaster в Picodata
## Общие сведения для установки
На данный момент функциональность распределенного SQL обеспечивается
только для приложений, использующих Tarantool Cartridge. Однако, при
этом требуется использовать версию Tarantool, которая поставляется
вместе с программным продуктом Picodata. Для использования
распределенного SQL требуется установить компонент SQL Broadcaster
согласно инструкциям в его
[Git-репозитории](https://git.picodata.io/picodata/picodata/sbroad).
Установка возможна либо путём компиляции исходного кода, либо
посредством утилиты `tarantoolctl`. В обоих случаях требуется выполнять
команды из директории Cartridge-приложения. Для ознакомления и
тестирования удобно воспользоваться [тестовым
приложением](https://git.picodata.io/picodata/picodata/sbroad/-/tree/main/sbroad-cartridge/test_app),
поставляемым вместе с кодом SQL Broadcaster.

## Пример установки для CentOS 7
Ниже показан пример установки сборочных зависимостей и компиляции
исходного кода SQL Broadcaster в операционной системе CentOS 7. Шаги для
других дистрибутивов Linux могут отличаться. Для начала требуется
подключить репозиторий с пакетами Picodata. Этот этап документирован для
[страницы загрузки Picodata](https://picodata.io/download/). Далее
предлагается выполнить в терминале следующие команды:

<style>
  code {
    white-space : pre-wrap !important;
    word-break: break-word;
  }
</style>

````bash
sudo yum --disablerepo="*" --enablerepo="picodata" install -y tarantool-picodata tarantool-picodata-devel
sudo yum install cartridge-cli
sudo yum groupinstall -y "Development Tools"
sudo yum install rust cargo cmake3
sudo ln -s /usr/bin/cmake3 /usr/local/bin/cmake
git clone https://git.picodata.io/picodata/picodata/sbroad.git
cd sbroad/sbroad-cartridge/test_app
````

Сборка SQL Broadcaster, запуск приложения, настройка тестового шардирования:
````bash
cd .. && make build_integration
cd test_app
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
````
## Пример использования
Перед тем как применять команды SQL Broadcaster, требуется загрузить в
тестовое приложение корректную схему данных. В схеме должны быть указаны
[спейсы](../glossary.md#спейс-space), формат колонок в них, а также
дополнительные параметры (движок хранения данных, ключ
[шардирования](../glossary.md#горизонтальное-масштабирование),
индексирование и т.д.). Для примера используем простую схему данных с
одним спейсом, предназначенным для хранения списка персонажей из
"Истории игрушек". Пусть там будут 4 колонки: идентификатор персонажа
(`id`), его имя (`name`), год появления (`year`) и служебная колонка
`bucket_id` для обозначения [бакета](../glossary.md#бакет-bucket), в
котором будет храниться запись. 

Чтобы применить схему данных, откройте веб-страницу Cartridge
(`localhost:8081`), перейдите в раздел Code и вставьте в поле файла
`schema.yml` следующее содержимое:
<details>
  <summary>Показать:</summary>

````bash
spaces:
    toy_story_characters:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: name
        is_nullable: false
      - type: integer
        name: year
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      - name
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
        
````
</details>

Не забудьте нажать кнопку `Apply`.

## Запись и чтение данных
Запросы на запись или чтение данных следует выполнять из консоли после
подключения к инстансу Tarantool. В данном тестовом примере для этого
достаточно подключиться к первому маршрутизатору:

````bash
cartridge enter router-1
````

После этого в строке приглашения можно вставить данные в спейс. За один раз можно добавить сразу несколько строк:
```
sbroad.execute([[insert into "toy_story_characters" ("id", "name", "year") values (?, ?, ?), (?, ?, ?)]], {1, "Woody", 1995, 2, "Forky", 2019})
```
Команда вернет общее число строк в спейсе:

```bash
---
- {'row_count': 2}
...
```

После того как данные записаны в таблицу, их можно прочитать:

```
sbroad.execute([[select "name" from "toy_story_characters" where "id" = 2]], {})
```

Вывод в консоли:

```bash
---
- {'metadata': [{'name': 'name', 'type': 'string'}], 'rows': [['Forky']]}
...
```


См. также: [Команды SQL](../sql_queries)

---
[Исходный код страницы](https://git.picodata.io/picodata/picodata/docs/-/blob/main/docs/sbroad/sql_tutorial.md)
