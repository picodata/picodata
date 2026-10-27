# Совместимость с ANSI

Приведенные в таблице возможности Picodata SQL соответствуют требованиям
стандарта SQL:2016, а именно ISO/IEC 9075:2016 «Информационные технологии
— Языки баз данных — SQL».

## Легенда {: #legend data-search-exclude }

!!! abstract ""
    <span class="full legend-id">E001-01</span><span class="legend-dash">—</span>возможность *полностью* реализована<br>
    <span class="partly legend-id">E001-01</span><span class="legend-dash">—</span>возможность *частично* реализована<br>
    <span class="absent legend-id">E001-01</span><span class="legend-dash">—</span>возможность не реализована

## Обязательные возможности {: #mandatory_features data-search-exclude }

<style>
.md-typeset .admonition.abstract {
    border-color: #9e9e9e;
}

.md-typeset .abstract > .admonition-title {
    background-color: #9e9e9e1a;
}

.md-typeset .abstract > .admonition-title::before {
    background-color: #9e9e9e;
}

td {
    align-content: center;
    padding: 0.75em 0.5em !important;
    line-height: 1.3;
}

td.td3 {
    white-space: nowrap;
}

td.td3 ul {
    list-style-type: none;
}

.td3 ul,
.td3 ul > li {
    margin-left: 0 !important;
}

.tr-header td {
    font-size: 1.25em;
}

.tr-header > td > span {
    font-family: revert;
}

.tr-header > td.td3 {
    font-size: revert;
}

.center {
    text-align: center !important;
}

.legend-id {
    line-height: 2.5em;
    margin-left: 0.5em;
}

.legend-dash {
    margin: 0.5em;
}

.full,
.absent,
.partly {
    padding: 0.2em 1em;
    border-radius: 1em;
    font-family: monospace;
}

.full {
    background-color: #d9ead3;
}

.absent {
    background-color: #f4cccc;
}

.partly {
    background-color: #fff2cc;
}

.fill-width {
    width: 1000px;
}
</style>

<table markdown="span">
    <thead>
        <tr>
            <th class="center">Идентификатор</th>
            <th class="center">Наименование</th>
            <th class="center">Поддержка</th>
            <th class="center">Примечание</th>
        </tr>
    </thead>
    <tbody>
<!-- E011 Numeric data types -->
        <tr class="tr-header">
            <td class="center">**E011**</td>
            <td colspan="3">**Числовые типы данных**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E011-01</span></td>
            <td>Типы данных INTEGER и SMALLINT (включая все варианты написания)</td>
            <td class="td3 center">[INTEGER, INT, SMALLINT](sql_types.md#integer)</td>
            <td>SMALLINT, INT2, INT4, INT8 и BIGINT — синонимы 64-битного INTEGER</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-02</span></td>
            <td>Типы данных REAL, DOUBLE PRECISION и FLOAT</td>
            <td class="td3 center">[DOUBLE, FLOAT8](sql_types.md#double)</td>
            <td>REAL, DOUBLE PRECISION и FLOAT не поддерживаются; доступны DOUBLE и его синоним FLOAT8</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-03</span></td>
            <td>Типы данных DECIMAL и NUMERIC</td>
            <td class="td3 center">[DECIMAL, NUMERIC](sql_types.md#decimal)</td>
            <td>NUMERIC — синоним DECIMAL. Указанные в объявлении точность и масштаб не ограничивают сохраняемые значения</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-04</span></td>
            <td>Арифметические операторы</td>
            <td class="td3 center"></td>
            <td>Унарные плюс и минус допустимы только в числовых литералах; выражения вида `-a` и `-(1 + 2)` не поддерживаются</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E011-05</span></td>
            <td>Числовые сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-06</span></td>
            <td>Неявное приведение числовых типов данных</td>
            <td class="td3 center"></td>
            <td>Числовые типы неявно приводятся в выражениях и сравнениях. При записи в колонку тип значения должен совпадать с типом колонки: например, вставка `6.` в колонку INTEGER отклоняется</td>
        </tr>
<!-- E021 Character string types -->
        <tr class="tr-header">
            <td class="center">**E021**</td>
            <td colspan="3">**Типы символьных строк**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-01</span></td>
            <td>Тип данных CHARACTER (включая все варианты написания)</td>
            <td class="td3 center"></td>
            <td>Строковые типы фиксированной длины CHARACTER и CHAR не поддерживаются; используйте TEXT или VARCHAR</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E021-02</span></td>
            <td>Тип данных CHARACTER VARYING (включая все варианты написания)</td>
            <td class="td3 center">[TEXT, STRING, VARCHAR](sql_types.md#text)</td>
            <td>CHARACTER VARYING и CHAR VARYING не поддерживаются. VARCHAR — синоним TEXT; ограничение длины VARCHAR(n) не применяется</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-03</span></td>
            <td>Символьные литералы</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-04</span></td>
            <td>Функция CHARACTER_LENGTH</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-05</span></td>
            <td>Функция OCTET_LENGTH</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E021-06</span></td>
            <td>Функция SUBSTRING</td>
            <td class="td3 center"><ul>
                                   <li>[SUBSTR](sql/substr.md)</li>
                                   <li>[SUBSTRING](sql/substring.md)</li>
                                   </ul></td>
            <td>Поддерживаются варианты FROM/FOR и поиск по регулярным выражениям. Числовые варианты используют SUBSTR; предложение USING не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-07</span></td>
            <td>Конкатенация символов</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-08</span></td>
            <td>Функции UPPER и LOWER</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E021-09</span></td>
            <td>Функция TRIM</td>
            <td class="td3 center">[TRIM](sql/trim.md)</td>
            <td>Форма `TRIM(FROM x)` без направления и удаляемых символов не поддерживается; используйте `TRIM(x)`</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-10</span></td>
            <td>Неявное приведение между типами символьных строк фиксированной и переменной длины</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-11</span></td>
            <td>Функция POSITION</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-12</span></td>
            <td>Сравнение символов</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E031 Identifiers -->
        <tr class="tr-header">
            <td class="center">**E031**</td>
            <td colspan="3">**Идентификаторы**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E031-01</span></td>
            <td>Идентификаторы с разделителями</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E031-02</span></td>
            <td>Идентификаторы в нижнем регистре</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E031-03</span></td>
            <td>Символ нижнего подчеркивания в конце</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E051 Basic query specification -->
        <tr class="tr-header">
            <td class="center">**E051**</td>
            <td colspan="3">**Спецификация базовых запросов**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-01</span></td>
            <td>SELECT DISTINCT</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   SELECT DISTINCT</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-02</span></td>
            <td>Предложение GROUP BY</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   GROUP BY</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-04</span></td>
            <td>GROUP BY может содержать колонки не из списка выборки</td>
            <td class="td3 center">[GROUP BY](sql/groupby.md)</td>
            <td>Колонки группировки можно не включать в список SELECT, например `SELECT COUNT(*) FROM t GROUP BY x`</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-05</span></td>
            <td>Элементы в списке выборки можно переименовывать</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-06</span></td>
            <td>Предложение HAVING</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   HAVING</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-07</span></td>
            <td>Допускается использование * в квалификаторе для списка выборки</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E051-08</span></td>
            <td>Корреляционные имена в предложении FROM</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E051-09</span></td>
            <td>Переименование колонок в предложении FROM</td>
            <td class="td3 center"></td>
            <td>Псевдоним таблицы поддерживается, список новых имен колонок после него — нет</td>
        </tr>
<!-- E061 Basic predicates and search conditions -->
        <tr class="tr-header">
            <td class="center">**E061**</td>
            <td colspan="3">**Базовые предикаты и условия поиска**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-01</span></td>
            <td>Предикат сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-02</span></td>
            <td>Предикат BETWEEN</td>
            <td class="td3 center">[Выражение](sql/select.md#expression) -><br>
                                   BETWEEN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-03</span></td>
            <td>Предикат IN со списком значений</td>
            <td class="td3 center">[Выражение](sql/select.md#expression) -><br>
                                   IN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E061-04</span></td>
            <td>Предикат LIKE</td>
            <td class="td3 center">[LIKE](sql/like.md)</td>
            <td>Форма `NOT LIKE` не поддерживается; отрицание записывается как `NOT (x LIKE y)`</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-05</span></td>
            <td>Предикат LIKE: предложение ESCAPE</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-06</span></td>
            <td>Предикат NULL</td>
            <td class="td3 center">[Выражение](sql/select.md#expression) -><br>
                                   NULL</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E061-07</span></td>
            <td>Предикат количественного сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-08</span></td>
            <td>Предикат EXISTS</td>
            <td class="td3 center">[Выражение](sql/select.md#expression) -><br>
                                   EXISTS</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-09</span></td>
            <td>Подзапросы в предикате сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-11</span></td>
            <td>Подзапросы в предикате IN</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E061-12</span></td>
            <td>Подзапросы в предикате количественного сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E061-13</span></td>
            <td>Коррелирующие подзапросы</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E061-14</span></td>
            <td>Условие поиска</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E071 Basic query expressions -->
        <tr class="tr-header">
            <td class="center">**E071**</td>
            <td colspan="3">**Базовые выражения с запросами**</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E071-01</span></td>
            <td>Табличный оператор UNION DISTINCT</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   UNION</td>
            <td>UNION устраняет повторы, но явное ключевое слово DISTINCT после UNION не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E071-02</span></td>
            <td>Табличный оператор UNION ALL</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   UNION ALL</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E071-03</span></td>
            <td>Табличный оператор EXCEPT DISTINCT</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   EXCEPT [ DISTINCT ]</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E071-05</span></td>
            <td>Колонки, объединенные с помощью табличных операторов, необязательно должны иметь одинаковый тип данных</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E071-06</span></td>
            <td>Табличные операторы в подзапросах</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E081 Basic Privileges -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**E081**</span></td>
            <td>**Базовые привилегии**</td>
            <td class="td3 center">[GRANT](sql/grant.md), [REVOKE](sql/revoke.md)</td>
            <td>Используются привилегии READ, WRITE и EXECUTE. Отдельные права INSERT, UPDATE, DELETE, права на колонки, REFERENCES и WITH GRANT OPTION не поддерживаются</td>
        </tr>
<!-- E091 Set functions -->
        <tr class="tr-header">
            <td class="center">**E091**</td>
            <td colspan="3">**Функции множеств**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-01</span></td>
            <td>AVG</td>
            <td class="td3 center">[AVG](sql/avg.md#syntax)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-02</span></td>
            <td>COUNT</td>
            <td class="td3 center">[COUNT](sql/count.md#syntax)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-03</span></td>
            <td>MAX</td>
            <td class="td3 center">[MAX](sql/max.md#syntax)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-04</span></td>
            <td>MIN</td>
            <td class="td3 center">[MIN](sql/min.md#syntax)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-05</span></td>
            <td>SUM</td>
            <td class="td3 center"><ul>
                                   <li>[SUM](sql/sum.md#syntax)</li>
                                   <li>[TOTAL](sql/total.md#syntax)</li>
                                   </ul></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E091-06</span></td>
            <td>Квантификатор ALL</td>
            <td class="td3 center"></td>
            <td>Агрегаты учитывают повторы по умолчанию, но явное ключевое слово ALL в их аргументах не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-07</span></td>
            <td>Квантификатор DISTINCT</td>
            <td class="td3 center">[DISTINCT](sql/groupby.md#aggregate_distinct)</td>
            <td></td>
        </tr>
<!-- E101 Basic data manipulation -->
        <tr class="tr-header">
            <td class="center">**E101**</td>
            <td colspan="3">**Базовые операции с данными**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E101-01</span></td>
            <td>Инструкция INSERT</td>
            <td class="td3 center">[INSERT](sql/insert.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E101-03</span></td>
            <td>Инструкция UPDATE с возможностью поиска</td>
            <td class="td3 center">[UPDATE](sql/update.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E101-04</span></td>
            <td>Инструкция DELETE с возможностью поиска</td>
            <td class="td3 center">[DELETE](sql/delete.md)</td>
            <td></td>
        </tr>
<!-- E111 Single row SELECT statement -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E111**</span></td>
            <td>**Инструкция SELECT, возвращающая одну строку**</td>
            <td class="td3 center"></td>
            <td>Речь об инструкции `SELECT ... INTO`, которая записывает результат в переменные; она не поддерживается</td>
        </tr>
<!-- E121 Basic cursor support -->
        <tr class="tr-header">
            <td class="center">**E121**</td>
            <td colspan="3">**Базовая поддержка курсоров**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-01</span></td>
            <td>DECLARE CURSOR</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-02</span></td>
            <td>Колонки ORDER BY необязательно должны быть в списке выборки</td>
            <td class="td3 center"></td>
            <td>ORDER BY разрешает ссылки на колонки результата SELECT; произвольные колонки исходной таблицы вне списка выборки недоступны</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E121-03</span></td>
            <td>Выражения значений в предложении ORDER BY</td>
            <td class="td3 center">[ORDER BY](sql/select.md#syntax)</td>
            <td>Поддерживаются выражения над колонками результата SELECT. Отдельный параметр и выражения без ссылок на колонки или подзапросы отклоняются</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-04</span></td>
            <td>Инструкция OPEN</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-06</span></td>
            <td>Инструкция UPDATE с возможностью позиционирования</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-07</span></td>
            <td>Инструкция DELETE с возможностью позиционирования</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-08</span></td>
            <td>Инструкция CLOSE</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-10</span></td>
            <td>Инструкция FETCH: неявный NEXT</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-17</span></td>
            <td>Курсоры WITH HOLD</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E131 Null value support (nulls in lieu of values) -->
        <tr class="tr-header">
            <td class="center"><span class="full">**E131**</span></td>
            <td>**Поддержка значения NULL (NULL вместо значений)**</td>
            <td class="td3 center">[NULL](sql_types.md)</td>
            <td></td>
        </tr>
<!-- E141 Basic integrity constraints -->
        <tr class="tr-header">
            <td class="center">**E141**</td>
            <td colspan="3">**Базовые ограничения целостности**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E141-01</span></td>
            <td>Ограничения NOT NULL</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md#syntax) -><br>
                                   [ NOT ] NULL</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E141-02</span></td>
            <td>Ограничения UNIQUE для колонок NOT NULL</td>
            <td class="td3 center">[CREATE UNIQUE INDEX](sql/create_index.md#params)</td>
            <td>Уникальность обеспечивается отдельным индексом; ограничение UNIQUE в CREATE TABLE не поддерживается. Для шардированной таблицы уникальный индекс должен начинаться со всех колонок ключа шардирования</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E141-03</span></td>
            <td>Ограничения PRIMARY KEY</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md#syntax) -><br>
                                   PRIMARY KEY</td>
            <td>Первичный ключ гарантирует уникальность только в пределах одного экземпляра кластера. Глобально уникален ключ, который включает все колонки ключа шардирования</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E141-04</span></td>
            <td>Базовое ограничение FOREIGN KEY с параметром NO ACTION по умолчанию
                как для ссылочной операции удаления, так и для ссылочной операции обновления</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E141-06</span></td>
            <td>Ограничения CHECK</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E141-07</span></td>
            <td>Значения колонок по умолчанию</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E141-08</span></td>
            <td>Ограничения NOT NULL неявно используются с ограничениями PRIMARY KEY</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md#syntax) -><br>
                                   PRIMARY KEY</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E141-10</span></td>
            <td>Имена во внешнем ключе могут указываться в любом порядке</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E151 Transaction support -->
        <tr class="tr-header">
            <td class="center">**E151**</td>
            <td colspan="3">**Поддержка транзакций**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E151-01</span></td>
            <td>Инструкция COMMIT</td>
            <td class="td3 center">[Ограничения транзакций](../tutorial/connecting.md#pgproto_limitations)</td>
            <td>COMMIT принимается как заглушка. Атомарное выполнение нескольких запросов доступно через [транзакционный блок DO](sql/transactions.md)</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E151-02</span></td>
            <td>Инструкция ROLLBACK</td>
            <td class="td3 center">[Ограничения транзакций](../tutorial/connecting.md#pgproto_limitations)</td>
            <td>ROLLBACK принимается как заглушка. Атомарное выполнение нескольких запросов доступно через [транзакционный блок DO](sql/transactions.md)</td>
        </tr>
<!-- E152 Basic SET TRANSACTION statement -->
        <tr class="tr-header">
            <td class="center">**E152**</td>
            <td colspan="3">**Базовая инструкция SET TRANSACTION**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E152-01</span></td>
            <td>Инструкция SET TRANSACTION: предложение ISOLATION LEVEL SERIALIZABLE</td>
            <td class="td3 center"></td>
            <td>SET TRANSACTION принимается, но не изменяет параметры транзакции</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E152-02</span></td>
            <td>Инструкция SET TRANSACTION: предложения READ ONLY и READ WRITE</td>
            <td class="td3 center"></td>
            <td>SET TRANSACTION принимается, но не изменяет параметры транзакции</td>
        </tr>
<!-- E153 Updatable queries with subqueries -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E153**</span></td>
            <td colspan="3">**Обновляемые запросы с подзапросами**</td>
        </tr>
<!-- E161 SQL comments using leading double minus -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E161**</span></td>
            <td>**Комментарии SQL, начинающиеся с двойного минуса**</td>
            <td class="td3 center"></td>
            <td>Серверный SQL-парсер не принимает комментарии. Удаление комментариев клиентом, например psql, не является поддержкой на сервере</td>
        </tr>
<!-- E171 SQLSTATE support -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**E171**</span></td>
            <td>**Поддержка SQLSTATE**</td>
            <td class="td3 center">[Протокол PostgreSQL](../tutorial/connecting.md#postgresql)</td>
            <td>Ответы об ошибках содержат пятисимвольные коды SQLSTATE. Набор кодов ограничен; многие ошибки SQL возвращаются как XX000 (internal_error)</td>
        </tr>
<!-- E182 Host language binding -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E182**</span></td>
            <td colspan="3">**Привязка к языку хоста**</td>
        </tr>
<!-- F021 Basic information schema -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F021**</span></td>
            <td>**Базовая информационная схема**</td>
            <td class="td3 center"></td>
            <td>Представления INFORMATION_SCHEMA отсутствуют; метаданные доступны в [системных таблицах](../architecture/system_tables.md)</td>
        </tr>
<!-- F031 Basic schema manipulation -->
        <tr class="tr-header">
            <td class="center">**F031**</td>
            <td colspan="3">**Базовые операции со схемой**</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F031-01</span></td>
            <td>Инструкция CREATE TABLE для создания постоянных базовых таблиц</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md)</td>
            <td>Первичный ключ обязателен: таблицу без PRIMARY KEY создать нельзя</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F031-02</span></td>
            <td>Инструкция CREATE VIEW</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F031-03</span></td>
            <td>Инструкция GRANT</td>
            <td class="td3 center">[GRANT](sql/grant.md)</td>
            <td>Используются привилегии Picodata (READ, WRITE и другие, см. E081); стандартные привилегии SELECT, INSERT, UPDATE и DELETE не поддерживаются</td>
        </tr>
        <tr>
            <td class="center"><span class="full">F031-04</span></td>
            <td>Инструкция ALTER TABLE: предложение ADD COLUMN</td>
            <td class="td3 center">[ALTER TABLE](sql/alter_table.md#params)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F031-13</span></td>
            <td>Инструкция DROP TABLE: предложение RESTRICT</td>
            <td class="td3 center">[DROP TABLE](sql/drop_table.md)</td>
            <td>Предложение RESTRICT не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F031-16</span></td>
            <td>Инструкция DROP VIEW: предложение RESTRICT</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F031-19</span></td>
            <td>Инструкция REVOKE: предложение RESTRICT</td>
            <td class="td3 center">[REVOKE](sql/revoke.md)</td>
            <td>Предложение RESTRICT не поддерживается</td>
        </tr>
<!-- F041 Basic joined table -->
        <tr class="tr-header">
            <td class="center">**F041**</td>
            <td colspan="3">**Базовая объединенная таблица**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">F041-01</span></td>
            <td>Внутреннее соединение (ключевое слово INNER необязательно)</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   [ INNER ] JOIN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F041-02</span></td>
            <td>Ключевое слово INNER</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   INNER JOIN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F041-03</span></td>
            <td>LEFT OUTER JOIN</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   LEFT OUTER JOIN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F041-04</span></td>
            <td>RIGHT OUTER JOIN</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F041-05</span></td>
            <td>Внешние соединения могут быть вложенными</td>
            <td class="td3 center">[SELECT —<br>
                                    Множественные<br>соединения](sql/select.md#multiple_joins)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F041-07</span></td>
            <td>Таблица, полученная левым или правым внешним соединением,
                может быть использована во внутреннем соединении</td>
            <td class="td3 center">[JOIN](sql/join.md)</td>
            <td>Таблица с NULL-дополнением в LEFT JOIN может участвовать в последующем INNER JOIN. RIGHT JOIN не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="full">F041-08</span></td>
            <td>Поддерживаются все операторы сравнения (не только обычное равенство)</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- F051 Basic date and time -->
        <tr class="tr-header">
            <td class="center">**F051**</td>
            <td colspan="3">**Базовая поддержка даты и времени**</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F051-01</span></td>
            <td>Тип данных DATE (включая поддержку литерала DATE)</td>
            <td class="td3 center">[DATETIME](sql_types.md#datetime), [типизированные литералы](sql/cast.md#examples)</td>
            <td>Тип DATE и литерал DATE '...' не поддерживаются. Доступны DATETIME и литерал DATETIME '...'</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-02</span></td>
            <td>Тип данных TIME (включая поддержку литерала TIME)
                с минимальной точностью дробной части секунд в 0 знаков</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-03</span></td>
            <td>Тип данных TIMESTAMP (включая поддержку литерала TIMESTAMP)
                с минимальной точностью дробной части секунд в 0 и 6 знаков</td>
            <td class="td3 center"></td>
            <td>Тип TIMESTAMP и литерал TIMESTAMP не поддерживаются; для хранения даты и времени используется DATETIME</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-04</span></td>
            <td>Предикат сравнения для типов данных DATE, TIME и TIMESTAMP</td>
            <td class="td3 center">[DATETIME](sql_types.md#datetime)</td>
            <td>Сравнивать значения DATETIME можно; стандартные типы DATE, TIME и TIMESTAMP отсутствуют</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F051-05</span></td>
            <td>Явное приведение с помощью CAST между типами даты и времени и типами символьных строк</td>
            <td class="td3 center">[CAST](sql/cast.md), [DATETIME](sql_types.md#datetime)</td>
            <td>Поддерживается приведение между DATETIME и TEXT. Стандартные типы DATE, TIME и TIMESTAMP отсутствуют</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F051-06</span></td>
            <td>Функция CURRENT_DATE</td>
            <td class="td3 center">[CURRENT_DATE](sql/current_date.md)</td>
            <td>Возвращает DATETIME со временем 00:00:00, а не DATE</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-07</span></td>
            <td>LOCALTIME</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F051-08</span></td>
            <td>LOCALTIMESTAMP</td>
            <td class="td3 center">[LOCALTIMESTAMP](sql/localtimestamp.md)</td>
            <td>Возвращает DATETIME с часовым поясом; результат совпадает с CURRENT_TIMESTAMP</td>
        </tr>
<!-- F081 UNION and EXCEPT in views -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F081**</span></td>
            <td colspan="3">**UNION и EXCEPT в представлениях**</td>
        </tr>
<!-- F131 Grouped operations -->
        <tr class="tr-header">
            <td class="center">**F131**</td>
            <td colspan="3">**Операции со сгруппированными представлениями**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F131-01</span></td>
            <td>Предложения WHERE, GROUP BY и HAVING поддерживаются
                в запросах со сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F131-02</span></td>
            <td>Несколько таблиц поддерживаются в запросах
                со сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F131-03</span></td>
            <td>Агрегатные функции поддерживаются в запросах
                со сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">F131-04</span></td>
            <td>Подзапросы с предложениями GROUP BY и HAVING и
                сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td>Подзапросы с GROUP BY и HAVING поддерживаются; представлений нет</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F131-05</span></td>
            <td>Инструкция SELECT, возвращающая одну строку, с предложениями
                GROUP BY и HAVING и сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- F181 Multiple module support -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F181**</span></td>
            <td colspan="3">**Поддержка нескольких модулей**</td>
        </tr>
<!-- F201 CAST function -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F201**</span></td>
            <td>**Функция CAST**</td>
            <td class="td3 center">[CAST](sql/cast.md)</td>
            <td></td>
        </tr>
<!-- F221 Explicit defaults -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F221**</span></td>
            <td colspan="3">**Явные значения по умолчанию**</td>
        </tr>
<!-- F261 CASE expression -->
        <tr class="tr-header">
            <td class="center">**F261**</td>
            <td colspan="3">**Выражение CASE**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">F261-01</span></td>
            <td>Простое выражение CASE</td>
            <td class="td3 center">[CASE —<br>
                                   Простое<br>выражение CASE](sql/case.md#case_simple)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F261-02</span></td>
            <td>Выражение CASE с условиями</td>
            <td class="td3 center">[CASE —<br>
                                   Выражение CASE<br>с условиями](sql/case.md#case_searched)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F261-03</span></td>
            <td>NULLIF</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F261-04</span></td>
            <td>COALESCE</td>
            <td class="td3 center">[COALESCE](sql/coalesce.md)</td>
            <td></td>
        </tr>
<!-- F311 Schema definition statement -->
        <tr class="tr-header">
            <td class="center">**F311**</td>
            <td colspan="3">**Инструкция определения схемы**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-01</span></td>
            <td>CREATE SCHEMA</td>
            <td class="td3 center"></td>
            <td>CREATE SCHEMA принимается как заглушка и не создает схему</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-02</span></td>
            <td>CREATE TABLE для постоянных базовых таблиц</td>
            <td class="td3 center"></td>
            <td>Самостоятельная команда CREATE TABLE поддерживается (F031-01); создание таблиц внутри CREATE SCHEMA не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-03</span></td>
            <td>CREATE VIEW</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-04</span></td>
            <td>CREATE VIEW: WITH CHECK OPTION</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-05</span></td>
            <td>Инструкция GRANT</td>
            <td class="td3 center"></td>
            <td>Самостоятельная команда GRANT поддерживается (F031-03); выдача привилегий внутри CREATE SCHEMA не поддерживается</td>
        </tr>
<!-- F471 Scalar subquery values -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F471**</span></td>
            <td>**Скалярные подзапросы**</td>
            <td class="td3 center"></td>
            <td>Подзапрос, возвращающий одно значение, можно использовать в выражениях, например `SELECT a + (SELECT MAX(b) FROM t2) FROM t1`. Коррелированные подзапросы не поддерживаются (E061-13)</td>
        </tr>
<!-- F481 Expanded NULL predicate -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**F481**</span></td>
            <td>**Расширенный предикат NULL**</td>
            <td class="td3 center"></td>
            <td>IS NULL применим к любому скалярному выражению; проверка строки из нескольких значений, например `(a, b) IS NULL`, не поддерживается</td>
        </tr>
<!-- F501 Features and conformance views -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F501**</span></td>
            <td colspan="3">**Представления возможностей и соответствия стандарту**</td>
        </tr>
<!-- F812 Basic flagging -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F812**</span></td>
            <td colspan="3">**Базовая диагностика отклонений от стандарта**</td>
        </tr>
<!-- S011 Distinct data types -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**S011**</span></td>
            <td colspan="3">**Пользовательские типы данных**</td>
        </tr>
<!-- T321 Basic SQL-invoked routines -->
        <tr class="tr-header">
            <td class="center">**T321**</td>
            <td colspan="3">**Базовые подпрограммы, вызываемые SQL**</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-01</span></td>
            <td>Пользовательские функции без перегрузки</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">T321-02</span></td>
            <td>Пользовательские хранимые процедуры без перегрузки</td>
            <td class="td3 center"><ul>
                                   <li>[ALTER PROCEDURE](sql/alter_procedure.md)</li>
                                   <li>[CREATE PROCEDURE](sql/create_procedure.md)</li>
                                   <li>[DROP PROCEDURE](sql/drop_procedure.md)</li>
                                   </ul></td>
            <td>Параметры задаются только типами, без имен и режимов IN, OUT, INOUT. Тело процедуры — одна команда INSERT, UPDATE или DELETE</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-03</span></td>
            <td>Вызов функции</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">T321-04</span></td>
            <td>Инструкция CALL</td>
            <td class="td3 center">[CALL](sql/call.md)</td>
            <td>Аргументами могут быть только литералы и параметры запроса, например `CALL p(1, $1)`; выражения вроде `1 + 2` не допускаются</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-05</span></td>
            <td>Инструкция RETURN</td>
            <td class="td3 center">[RETURN QUERY](sql/transactions.md#return_query)</td>
            <td>RETURN QUERY в блоке DO добавляет строки к результату и не завершает блок. Стандартная инструкция RETURN не реализована</td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-06</span></td>
            <td>Представление ROUTINES</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-07</span></td>
            <td>Представление PARAMETERS</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- T631 IN predicate with one list element -->
        <tr class="tr-header">
            <td class="center"><span class="full">**T631**</span></td>
            <td colspan="3">**Предикат IN с одним элементом списка**</td>
        </tr>
    </tbody>
</table>

## Необязательные возможности {: #optional_features data-search-exclude }

<table markdown="span">
    <thead>
        <tr>
            <th class="center">Идентификатор</th>
            <th class="center fill-width">Наименование</th>
            <th class="center fill-width">Поддержка</th>
            <th class="center" style="width:35%">Примечание</th>
        </tr>
    </thead>
    <tbody>
<!-- F200 TRUNCATE TABLE statement -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F200**</span></td>
            <td>**Инструкция TRUNCATE TABLE**</td>
            <td class="td3 center">[TRUNCATE TABLE](sql/truncate_table.md)</td>
            <td></td>
        </tr>
<!-- F321 User authorization -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**F321**</span></td>
            <td>**Авторизация пользователя**</td>
            <td class="td3 center">[CURRENT_USER](sql/current_user.md), [управление доступом](../admin/access_control.md)</td>
            <td>CURRENT_USER возвращает имя пользователя, с привилегиями которого выполняется запрос. SESSION_USER, SYSTEM_USER и SET SESSION AUTHORIZATION не поддерживаются</td>
        </tr>
<!-- F571 Truth value tests -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F571**</span></td>
            <td>**Проверка истинности значений**</td>
            <td class="td3 center">[Выражение](sql/select.md#expression)</td>
            <td>`IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN`</td>
        </tr>
<!-- F641 Row and table constructors -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**F641**</span></td>
            <td>**Конструкторы строк и таблиц**</td>
            <td class="td3 center">[VALUES](sql/values.md)</td>
            <td>Поддерживаются VALUES с несколькими строками и строки значений в сравнениях и IN, например `(a, b) = (1, 2)`. Строку нельзя использовать как самостоятельное значение</td>
        </tr>
<!-- F850 Top-level ORDER BY clause in query expression -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F850**</span></td>
            <td>**ORDER BY в запросе верхнего уровня**</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   ORDER BY</td>
            <td>Поддерживаются ASC, DESC, NULLS FIRST и NULLS LAST; ограничения ссылок на колонки описаны в E121-02 и E121-03</td>
        </tr>
<!-- F851 ORDER BY clause in subqueries -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F851**</span></td>
            <td>**ORDER BY в подзапросах**</td>
            <td class="td3 center"></td>
            <td>ORDER BY допускается в подзапросах и CTE</td>
        </tr>
<!-- S091 Basic array support (SQL:2016) -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**S091**</span></td>
            <td>**Базовая поддержка массивов**</td>
            <td class="td3 center">[ARRAY](sql_types.md#array), [литералы массивов](sql/values.md#examples)</td>
            <td>Поддерживаются одномерные массивы встроенных типов и `ARRAY[...]`. Массивы нельзя сравнивать и использовать как ключи шардирования; пользовательские типы элементов отсутствуют</td>
        </tr>
<!-- T031 BOOLEAN data type -->
        <tr class="tr-header">
            <td class="center"><span class="full">**T031**</span></td>
            <td>**Тип данных BOOLEAN**</td>
            <td class="td3 center">[BOOLEAN, BOOL](sql_types.md#boolean)</td>
            <td></td>
        </tr>
<!-- T121 WITH (excluding RECURSIVE) in query expression -->
        <tr class="tr-header">
            <td class="center"><span class="full">**T121**</span></td>
            <td>**WITH (без RECURSIVE) в запросе**</td>
            <td class="td3 center">[CTE](sql/with.md)</td>
            <td>Рекурсивные CTE (WITH RECURSIVE) не поддерживаются</td>
        </tr>
<!-- T331 Basic roles -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**T331**</span></td>
            <td>**Базовые роли**</td>
            <td class="td3 center"><ul>
                                   <li>[CREATE ROLE](sql/create_role.md)</li>
                                   <li>[DROP ROLE](sql/drop_role.md)</li>
                                   </ul></td>
            <td>Команда SET ROLE для смены активной роли не поддерживается</td>
        </tr>
<!-- T441 ABS and MOD functions -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**T441**</span></td>
            <td>**Функции ABS и MOD**</td>
            <td class="td3 center">[ABS](sql/abs.md)</td>
            <td>Функции MOD нет; остаток от деления вычисляет оператор `%`</td>
        </tr>
<!-- T611 Elementary OLAP operations -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**T611**</span></td>
            <td>**Элементарные операции OLAP (оконные функции)**</td>
            <td class="td3 center"><ul>
                                   <li>[Оконные функции](sql/window.md)</li>
                                   <li>[ROW_NUMBER](sql/row_number.md)</li>
                                   </ul></td>
            <td>Поддерживаются агрегатные оконные функции, ROW_NUMBER, PARTITION BY, ORDER BY, рамки ROWS и RANGE и именованные окна (WINDOW). Функций RANK и DENSE_RANK нет</td>
        </tr>
<!-- T617 FIRST_VALUE and LAST_VALUE functions -->
        <tr class="tr-header">
            <td class="center"><span class="partly">**T617**</span></td>
            <td>**Функции FIRST_VALUE и LAST_VALUE**</td>
            <td class="td3 center">[LAST_VALUE](sql/last_value.md)</td>
            <td>Функции FIRST_VALUE нет</td>
        </tr>
    </tbody>
</table>

## Дополняющие стандарт возможности {: #additional_features data-search-exclude }

<table markdown="span">
    <thead>
        <tr>
            <th class="center">Идентификатор</th>
            <th class="center fill-width">Наименование</th>
            <th class="center fill-width">Поддержка</th>
            <th class="center" style="width:35%">Примечание</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Тип данных TEXT</td>
            <td class="td3 center">[TEXT, STRING](sql_types.md#text)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Тип данных UUID</td>
            <td class="td3 center">[UUID](sql_types.md#uuid)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Тип данных JSON</td>
            <td class="td3 center">[JSON](sql_types.md#json)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Тип данных UNSIGNED</td>
            <td class="td3 center">[UNSIGNED](sql_types.md#unsigned)</td>
            <td>Целое число не меньше 0; используется только в определениях колонок</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>GROUP_CONCAT</td>
            <td class="td3 center">[GROUP_CONCAT](sql/group_concat.md#syntax)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>STRING_AGG</td>
            <td class="td3 center">[STRING_AGG](sql/string_agg.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>CREATE INDEX</td>
            <td class="td3 center">[CREATE INDEX](sql/create_index.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>DROP INDEX</td>
            <td class="td3 center">[DROP INDEX](sql/drop_index.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>LIMIT</td>
            <td class="td3 center">[SELECT](sql/select.md#limit) -><br>
                                   LIMIT</td>
            <td>Стандартные OFFSET и FETCH FIRST не поддерживаются</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>INDEXED BY</td>
            <td class="td3 center">[INDEXED BY](sql/indexed_by.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>ALTER SYSTEM</td>
            <td class="td3 center">[ALTER SYSTEM](sql/alter_system.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>ALTER USER</td>
            <td class="td3 center">[ALTER USER](sql/alter_user.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>CREATE USER</td>
            <td class="td3 center">[CREATE USER](sql/create_user.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>DROP USER</td>
            <td class="td3 center">[DROP USER](sql/drop_user.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>EXPLAIN</td>
            <td class="td3 center">[EXPLAIN](sql/explain.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>TO_CHAR</td>
            <td class="td3 center">[TO_CHAR](sql/to_char.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>TO_DATE</td>
            <td class="td3 center">[TO_DATE](sql/to_date.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>CURRENT_TIMESTAMP</td>
            <td class="td3 center">[CURRENT_TIMESTAMP](sql/current_timestamp.md)</td>
            <td>Возвращает DATETIME с часовым поясом</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>ILIKE</td>
            <td class="td3 center">[ILIKE](sql/ilike.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>ALTER INDEX</td>
            <td class="td3 center">[ALTER INDEX](sql/alter_index.md)</td>
            <td>Переименование индекса</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>JSON_EXTRACT_PATH</td>
            <td class="td3 center">[JSON_EXTRACT_PATH](sql/json_extract_path.md)</td>
            <td>Извлечение значения JSON по пути; не заменяет стандартные операторы SQL/JSON</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">—</span></td>
            <td>Транзакционные блоки DO</td>
            <td class="td3 center">[Транзакции](sql/transactions.md)</td>
            <td>Атомарное выполнение в пределах одного узла и общего бакета; доступны LET, IF/ELSE и RETURN QUERY. Действуют [ограничения](sql/transactions.md#limitations)</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">—</span></td>
            <td>INSERT ON CONFLICT DO UPDATE</td>
            <td class="td3 center">[ON CONFLICT DO UPDATE](sql/iocdu.md)</td>
            <td>Поддерживается внутри транзакционных блоков для шардированных таблиц; действуют ограничения на вставку и обновление</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>DISCARD</td>
            <td class="td3 center">[DISCARD](sql/discard.md)</td>
            <td>Очистка локальных кешей SQL на инстансе</td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Функции Picodata</td>
            <td class="td3 center">[Функции](sql/sql_functions.md)</td>
            <td>Сведения о кластере и инстансе: PICO_INSTANCE_NAME, PICO_LOG_LEVEL, PICO_INSTANCE_HEALTH_STATUS и другие</td>
        </tr>
    </tbody>
</table>
