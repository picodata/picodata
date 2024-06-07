# Параметризованные запросы

В [DQL](dql.md)- и [DML](dml.md)-запросах с условиями можно использовать
как обычный вид, так и параметризированный. В последнем случае
потребуется сначала переключить язык консоли на Lua. Так, следующие две
команды дадут одинаковый результат (вывод строки по известному `id`). :

```sql
SELECT item FROM warehouse WHERE id = 1;
```

```lua
pico.sql([[SELECT item FROM warehouse WHERE id = ?]], {1});
```

Вывод в консоль:

```
---
- metadata:
  - {'name': 'ITEM', 'type': 'string'}
  rows:
  - ['bricks']
...
```

Разница состоит в том, что при параметризации происходит кеширование
плана запроса по ключу от шаблона SQL (в данном случае `select item
from warehouse where id = ?`), и если подобных запросов несколько,
то они все смогут использовать кешированный план. Без параметризации у
каждого запроса будет свой отдельный план, и ускорения от кеша не
произойдет.

Пример вывода строк по нескольким условиям для разных столбцов (также
два варианта):

```sql
SELECT item, type FROM warehouse WHERE id > 3 AND type = 'light'
```

```lua
pico.sql([[SELECT item, type FROM warehouse WHERE id > ? AND type = ?]], {3, 'light'});
```

Вывод в консоль:

```
---
- metadata:
  - {'name': 'ITEM', 'type': 'string'}
  - {'name': 'TYPE', 'type': 'string'}
  rows:
  - ['piles', 'light']
  - ['panels', 'light']
...
```
