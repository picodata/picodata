# Совместимость с ANSI

Приведенные в таблице возможности Picodata SQL соответствуют требованиям
стандарта SQL:2016, а именно ISO/IEC 9075:2016 «Информационные технологии
— Языки баз данных — SQL».

!!! abstract "Легенда"
    <span class="full legend-id">E001-01</span><span class="legend-dash">—</span>возможность *полностью* реализована<br>
    <span class="partly legend-id">E001-01</span><span class="legend-dash">—</span>возможность *частично* реализована<br>
    <span class="absent legend-id">E001-01</span><span class="legend-dash">—</span>возможность не реализована

## Обязательные возможности {: #mandatory_features }

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
            <td class="center"><span class="partly">E011-01</span></td>
            <td>Типы данных INTEGER и SMALLINT (включая все варианты написания)</td>
            <td class="td3 center"><ul>
                                   <li>[INTEGER, INT](sql_types.md#integer)</li>
                                   <li>[UNSIGNED](sql_types.md#unsigned)</li>
                                   </ul></td>
            <td>Тип SMALLINT не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-02</span></td>
            <td>Типы данных REAL, DOUBLE PRECISION и FLOAT</td>
            <td class="td3 center">[DOUBLE](sql_types.md#double)</td>
            <td>Типы REAL, DOUBLE PRECISION, FLOAT не поддерживаются, но есть DOUBLE</td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E011-03</span></td>
            <td>Типы данных DECIMAL и NUMERIC</td>
            <td class="td3 center">[DECIMAL](sql_types.md#decimal)</td>
            <td>Тип NUMERIC не поддерживается</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E011-04</span></td>
            <td>Арифметические операторы</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E011-05</span></td>
            <td>Числовые сравнения</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E011-06</span></td>
            <td>Неявное приведение числовых типов данных</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E021 Character string types -->
        <tr class="tr-header">
            <td class="center">**E021**</td>
            <td colspan="3">**Типы символьных строк**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-01</span></td>
            <td>Тип данных CHARACTER (включая все варианты написания)</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="partly">E021-02</span></td>
            <td>Тип данных CHARACTER VARYING (включая все варианты написания)</td>
            <td class="td3 center">[VARCHAR](sql_types.md#varchar)</td>
            <td>Типы VARYING, CHARACTER VARYING не поддерживаются, но есть VARCHAR</td>
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
            <td class="center"><span class="absent">E021-06</span></td>
            <td>Функция SUBSTRING</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-07</span></td>
            <td>Конкатенация символов</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E021-08</span></td>
            <td>Функции UPPER и LOWER</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E021-09</span></td>
            <td>Функция TRIM</td>
            <td class="td3 center">[TRIM](sql/trim.md)</td>
            <td></td>
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
            <td class="center"><span class="absent">E051-04</span></td>
            <td>GROUP BY может содержать колонки не из списка выборки</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td></td>
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
            <td class="center"><span class="absent">E061-04</span></td>
            <td>Предикат LIKE</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E061-05</span></td>
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
            <td class="center"><span class="full">E071-01</span></td>
            <td>Табличный оператор UNION DISTINCT</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   UNION</td>
            <td></td>
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
            <td class="center"><span class="absent">**E081**</span></td>
            <td colspan="3">**Базовые привилегии**</td>
        </tr>
<!-- E091 Set functions -->
        <tr class="tr-header">
            <td class="center">**E091**</td>
            <td colspan="3">**Функции множеств**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-01</span></td>
            <td>AVG</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   AVG</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-02</span></td>
            <td>COUNT</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   COUNT</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-03</span></td>
            <td>MAX</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   MAX</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-04</span></td>
            <td>MIN</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   MIN</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-05</span></td>
            <td>SUM</td>
            <td class="td3 center"><ul>
                                   <li>[Агрегатные функции](sql/aggregate.md#syntax) -></li>
                                   <li>SUM</li>
                                   <li>TOTAL</li>
                                   </ul></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E091-06</span></td>
            <td>Квантификатор ALL</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E091-07</span></td>
            <td>Квантификатор DISTINCT</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   DISTINCT</td>
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
            <td class="center"><span class="full">**E111**</span></td>
            <td colspan="3">**Инструкция SELECT, возвращающая одну строку**</td>
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
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E121-03</span></td>
            <td>Выражения значений в предложении ORDER BY</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td>**Поддержка нулевого значения (нули вместо значений)**</td>
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
            <td class="center"><span class="absent">E141-02</span></td>
            <td>Ограничения UNIQUE для колонок NOT NULL</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">E141-03</span></td>
            <td>Ограничения PRIMARY KEY</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md#syntax) -><br>
                                   PRIMARY KEY</td>
            <td></td>
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
            <td class="center"><span class="absent">E141-08</span></td>
            <td>Ограничения NOT NULL неявно используются с ограничениями PRIMARY KEY</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E141-10</span></td>
            <td>Имена в во внешнем ключе могут указываться в любом порядке</td>
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
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E151-02</span></td>
            <td>Инструкция ROLLBACK</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">E152-02</span></td>
            <td>Инструкция SET TRANSACTION: предложения READ ONLY и READ WRITE</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
<!-- E153 Updatable queries with subqueries -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E153**</span></td>
            <td colspan="3">**Обновляемые запросы с подзапросами**</td>
        </tr>
<!-- E161 SQL comments using leading double minus -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E161**</span></td>
            <td colspan="3">**Комментарии SQL, начинающиеся с двойного минуса**</td>
        </tr>
<!-- E171 SQLSTATE support -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E171**</span></td>
            <td colspan="3">**Поддержка SQLSTATE**</td>
        </tr>
<!-- E182 Host language binding -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**E182**</span></td>
            <td colspan="3">**Привязка к языку хоста**</td>
        </tr>
<!-- F031 Basic schema manipulation -->
        <tr class="tr-header">
            <td class="center">**F031**</td>
            <td colspan="3">**Базовые операции со схемой**</td>
        </tr>
        <tr>
            <td class="center"><span class="full">F031-01</span></td>
            <td>Инструкция CREATE TABLE для создания постоянных базовых таблиц</td>
            <td class="td3 center">[CREATE TABLE](sql/create_table.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F031-02</span></td>
            <td>Инструкция CREATE VIEW</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F031-03</span></td>
            <td>Инструкция GRANT</td>
            <td class="td3 center">[GRANT](sql/grant.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F031-04</span></td>
            <td>Инструкция ALTER TABLE: предложение ADD COLUMN</td>
            <td class="td3 center"></td>
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
            <td class="center"><span class="absent">F041-07</span></td>
            <td>Таблица, полученная левым или правым внешним соединением,
                может быть использована во внутреннем соединении</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td class="td3 center">[DATETIME](sql_types.md#datetime)</td>
            <td>Тип и литерал DATE не поддерживаются, но есть DATETIME</td>
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
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-04</span></td>
            <td>Предикат сравнения для типов данных DATE, TIME и TIMESTAMP</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-05</span></td>
            <td>Явное приведение с помощью CAST между типами даты и времени и типами символьных строк</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">F051-06</span></td>
            <td>Функция CURRENT_DATE</td>
            <td class="td3 center">[CURRENT_DATE](sql/current_date.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-07</span></td>
            <td>LOCALTIME</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F051-08</span></td>
            <td>LOCALTIMESTAMP</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td class="center"><span class="absent">F131-04</span></td>
            <td>Подзапросы с предложениями GROUP BY и HAVING и
                сгруппированными представлениями</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td class="center"><span class="full">**F201**<spam></td>
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
            <td class="center"><span class="absent">F261-04</span></td>
            <td>COALESCE</td>
            <td class="td3 center"></td>
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
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">F311-02</span></td>
            <td>CREATE TABLE для постоянных базовых таблиц</td>
            <td class="td3 center"></td>
            <td></td>
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
            <td></td>
        </tr>
<!-- F471 Scalar subquery values -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F471**</span></td>
            <td>**Скалярные значения подзапросов**</td>
            <td class="td3 center">[SCALAR](sql_types.md#scalar)</td>
            <td></td>
        </tr>
<!-- F481 Expanded NULL predicate -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F481**</span></td>
            <td colspan="3">**Расширенный предикат NULL**</td>
        </tr>
<!-- F812 Basic flagging -->
        <tr class="tr-header">
            <td class="center"><span class="absent">**F812**</span></td>
            <td colspan="3">**Базовое использование флагов**</td>
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
            <td class="center"><span class="full">T321-02</span></td>
            <td>Пользовательские хранимые процедуры без перегрузки</td>
            <td class="td3 center"><ul>
                                   <li>[ALTER PROCEDURE](sql/alter_procedure.md)</li>
                                   <li>[CREATE PROCEDURE](sql/create_procedure.md)</li>
                                   <li>[DROP PROCEDURE](sql/drop_procedure.md)</li>
                                   </ul></td>
            <td><alter routine statement></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-03</span></td>
            <td>Вызов функции</td>
            <td class="td3 center"></td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">T321-04</span></td>
            <td>Инструкция CALL</td>
            <td class="td3 center">[CALL](sql/call.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="absent">T321-05</span></td>
            <td>Инструкция RETURN</td>
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

## Необязательные возможности {: #optional_features }

<table markdown="span">
    <thead>
        <tr>
            <th class="center">Идентификатор</th>
            <th class="center fill-width">Наименование</th>
            <th class="center fill-width">Поддержка</th>
            <th class="center">Примечание</th>
        </tr>
    </thead>
    <tbody>
<!-- F321 User authorization -->
        <tr class="tr-header">
            <td class="center"><span class="full">**F321**</span></td>
            <td colspan="3">**Авторизация пользователя**</td>
        </tr>
<!-- T031 BOOLEAN data type -->
        <tr class="tr-header">
            <td class="center"><span class="full">**T031**</span></td>
            <td>**Тип данных BOOLEAN**</td>
            <td class="td3 center">[BOOLEAN, BOOL](sql_types.md#boolean)</td>
            <td></td>
        </tr>
<!-- T331 Basic roles -->
        <tr class="tr-header">
            <td class="center"><span class="full">**T331**</span></td>
            <td>**Базовые роли**</td>
            <td class="td3 center"><ul>
                                   <li>[CREATE ROLE](sql/create_role.md)</li>
                                   <li>[DROP ROLE](sql/drop_role.md)</li>
                                   </ul></td>
            <td></td>
        </tr>
    </tbody>
</table>

## Дополняющие стандарт возможности {: #additional_features }

<table markdown="span">
    <thead>
        <tr>
            <th class="center">Идентификатор</th>
            <th class="center fill-width">Наименование</th>
            <th class="center fill-width">Поддержка</th>
            <th class="center">Примечание</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>Тип данных NUMBER</td>
            <td class="td3 center">[NUMBER](sql_types.md#number)</td>
            <td></td>
        </tr>
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
            <td>GROUP_CONCAT</td>
            <td class="td3 center">[Агрегатные функции](sql/aggregate.md#syntax) -><br>
                                   GROUP_CONCAT</td>
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
            <td>ORDER BY</td>
            <td class="td3 center">[SELECT](sql/select.md#syntax) -><br>
                                   ORDER BY</td>
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
            <td>VALUES</td>
            <td class="td3 center">[VALUES](sql/values.md)</td>
            <td></td>
        </tr>
        <tr>
            <td class="center"><span class="full">—</span></td>
            <td>CTE</td>
            <td class="td3 center">CTE</td>
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
            <td>CURRENT_DATE</td>
            <td class="td3 center">CURRENT_DATE</td>
            <td></td>
        </tr>
    </tbody>
</table>
