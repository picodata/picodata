# Пример работы с SQL Broadcaster в Picodata

cartridge replicasets setup --bootstrap-vshard
````
## Пример использования
Перед тем как применять команды SQL Broadcaster, требуется загрузить в
тестовое приложение корректную схему данных. В схеме должны быть указаны
[спейсы](../glossary.md#space), формат колонок в них, а также
дополнительные параметры (движок хранения данных, ключ
[шардирования](../glossary.md#sharding),
индексирование и т.д.). Для примера используем простую схему данных с
одним спейсом, предназначенным для хранения списка персонажей из
"Истории игрушек". Пусть там будут 4 колонки: идентификатор персонажа
(`id`), его имя (`name`), год появления (`year`) и служебная колонка
`bucket_id` для обозначения [бакета](../glossary.md#bucket), в
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


См. также: [Команды SQL](../queries)

---
[Исходный код страницы](https://git.picodata.io/picodata/picodata/docs/-/blob/main/docs/sql/tutorial.md)
