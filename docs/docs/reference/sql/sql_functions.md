# Функции и выражения SQL

Встроенные SQL-функции и выражения Picodata в алфавитном порядке.
Названия ведут к подробному описанию синтаксиса и ограничений.

| Функция или выражение | Описание |
|---------|----------|
| [_PICO_BUCKET](_pico_bucket.md) | Диапазоны бакетов и их распределение по репликасетам указанного тира в формате JSON |
| [ABS](abs.md) | Абсолютное значение числа |
| [AVG](avg.md) | Среднее значение |
| [CASE](case.md) | Выбор результата по условию или значению выражения |
| [CAST](cast.md) | Приведение значения к указанному типу данных |
| [COALESCE](coalesce.md) | Первый аргумент, отличный от `NULL`; `NULL`, если все аргументы равны `NULL` |
| [COUNT](count.md) | Количество строк (`COUNT(*)`) или значений, отличных от `NULL` (`COUNT(expression)`) |
| [CURRENT_DATE](current_date.md) | Текущая дата с временем `00:00:00` и часовым поясом; результат имеет тип `DATETIME` |
| [CURRENT_TIMESTAMP](current_timestamp.md) | Текущая дата и время с часовым поясом |
| [CURRENT_USER](current_user.md) | Имя пользователя, с привилегиями которого выполняется запрос |
| [GROUP_CONCAT](group_concat.md) | Объединение строковых значений с заданным разделителем |
| [ILIKE](ilike.md) | Проверка соответствия строки шаблону без учета регистра |
| [JSON_EXTRACT_PATH](json_extract_path.md) | Извлечение данных из JSON по компонентам пути |
| [LAST_VALUE](last_value.md) | Значение выражения в последней строке рамки окна |
| [LIKE](like.md) | Проверка соответствия строки шаблону с учетом регистра |
| [LOCALTIMESTAMP](localtimestamp.md) | Текущая дата и время с часовым поясом и заданной точностью дробной части секунд |
| [LOWER](lower.md) | Преобразование строки в нижний регистр |
| [MAX](max.md) | Максимальное значение |
| [MIN](min.md) | Минимальное значение |
| [PICO_CONFIG_FILE_PATH](pico_config_file_path.md) | Путь к файлу конфигурации инстанса по его UUID; `NULL`, если файл не использовался |
| [PICO_INSTANCE_DIR](pico_instance_dir.md) | Путь к рабочей директории инстанса по его UUID |
| [PICO_INSTANCE_HEALTH_STATUS](pico_instance_health_status.md) | Сведения о состоянии инстанса в формате JSON по его UUID |
| [PICO_INSTANCE_NAME](pico_instance_name.md) | Имя инстанса по его UUID |
| [PICO_INSTANCE_UUID](pico_instance_uuid.md) | UUID текущего инстанса в текстовом виде |
| [PICO_LOG_LEVEL](pico_log_level.md) | Текущий уровень журналирования инстанса в текстовом виде |
| [PICO_LOG_LEVEL_MAP](pico_log_level_map.md) | Соответствие названий уровней журналирования их числовым значениям в формате JSON |
| [PICO_RAFT_LEADER_ID](pico_raft_leader_id.md) | Идентификатор лидера raft-группы |
| [PICO_RAFT_LEADER_UUID](pico_raft_leader_uuid.md) | UUID лидера raft-группы в текстовом виде |
| [PICO_REPLICASET_NAME](pico_replicaset_name.md) | Имя репликасета инстанса по его UUID |
| [PICO_TIER_NAME](pico_tier_name.md) | Имя тира инстанса по его UUID |
| [ROW_NUMBER](row_number.md) | Номер строки в разделе окна, начиная с единицы |
| [STRING_AGG](string_agg.md) | Другое имя `GROUP_CONCAT` для совместимости с PostgreSQL |
| [SUBSTR](substr.md) | Извлечение подстроки по начальной позиции и длине; нумерация начинается с единицы |
| [SUBSTRING](substring.md) | Извлечение подстроки по позиции и длине или по регулярному выражению |
| [SUM](sum.md) | Сумма значений; `NULL`, если строк нет или все значения равны `NULL` |
| [TOTAL](total.md) | Сумма значений с результатом типа `DOUBLE`; `0.0`, если строк нет или все значения равны `NULL` |
| [TO_CHAR](to_char.md) | Преобразование значения `DATETIME` в строку заданного формата |
| [TO_DATE](to_date.md) | Преобразование строки заданного формата в значение `DATETIME` |
| [TRIM](trim.md) | Удаление указанных символов в начале, в конце или с обеих сторон строки |
| [UPPER](upper.md) | Преобразование строки в верхний регистр |
| [VERSION](version.md) | Версия Picodata текущего инстанса |
