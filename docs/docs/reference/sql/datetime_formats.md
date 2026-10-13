# Форматы даты и времени

<div class="datetime-formats" markdown="1">

Форматы строк, которые принимает тип [DATETIME].

Например:

- `2025-10-18` — дата
- `2025-10-18 02:59:59` — дата и время
- `2025-10-18T02:59:59.52Z` — дата и время с долей секунды в UTC
- `2025-10-18 02:59:59 Europe/Moscow` — дата и время с часовым поясом

В таблицах близкие варианты записи объединены в одной строке.
Значения `YYYY`, `Mon`, `<DATE>` и других обозначений описаны в
[общих правилах](#notation).

## Дата {: #date }

| Формат | Пример | Примечание |
| --- | --- | --- |
| `YYYY-MM-DD`<br>`YYYY/MM/DD`<br>`YYYY.MM.DD`<br>`YYYY MM DD` | `2025-10-18` | Год, месяц, день |
| `YYYYMMDD`<br>`YYMMDD` | `20251018`<br>`251018` | Без разделителей |
| `MM-DD-YYYY`<br>`MM/DD/YYYY`<br>`MM.DD.YYYY`<br>`MM DD YYYY`<br>`MM-DD-YY`<br>`MM/DD/YY`<br>`MM.DD.YY`<br>`MM DD YY` | `10/18/2025`<br>`10/18/25` | Месяц, день, год |
| `YYYY-DDD`<br>`YYYY/DDD`<br>`YYYY.DDD`<br>`YYYY DDD` | `2025-291` | Год и номер дня в году |
| `Mon DD YYYY`<br>`Mon DD, YYYY`<br>`Mon-DD-YYYY`<br>`Mon/DD/YYYY`<br>`Mon.DD.YYYY`<br>`MonDD YYYY`<br>`Mon DD YY`<br>`Mon DD, YY`<br>`Mon-DD-YY`<br>`Mon/DD/YY`<br>`Mon.DD.YY` | `Oct 18, 2025`<br>`Oct18 2025` | Месяц словом, день, год |
| `DD Mon YYYY`<br>`DD-Mon-YYYY`<br>`DD/Mon/YYYY`<br>`DD.Mon.YYYY`<br>`DDMonYYYY`<br>`DD Mon YY`<br>`DD-Mon-YY`<br>`DD/Mon/YY`<br>`DD.Mon.YY`<br>`DDMonYY` | `18 Oct 2025`<br>`18Oct25` | День, месяц словом, год |
| `YYYY Mon DD`<br>`YYYY-Mon-DD`<br>`YYYY/Mon/DD`<br>`YYYY.Mon.DD`<br>`YYYYMonDD` | `2025 Oct 18` | Год, месяц словом, день |
| `Mon YYYY DD`<br>`Mon-YYYY-DD`<br>`Mon/YYYY/DD`<br>`Mon.YYYY.DD`<br>`YYYY DD Mon` | `Oct 2025 18`<br>`2025 18 Oct` | Другие порядки с месяцем словом |
| `JN`<br>`J N`<br>`JDN`<br>`JD N`<br>`JulianN`<br>`Julian N` | `J2460967` | Юлианский день |

## Время {: #time }

Форматы времени используются в [сочетаниях с датой](#datetime).

| Формат | Пример | Примечание |
| --- | --- | --- |
| `HH:MM`<br>`HH:MM:SS`<br>`HH:MM:SS.F`<br>`MM:SS.F` | `02:59`<br>`02:59:59.5` | С двоеточием. Запись `59:59.5` означает минуты и секунды с дробной частью. |
| `HHMM`<br>`HHMM.F`<br>`HHMMSS`<br>`HHMMSS.F` | `0259`<br>`025959.5` | Без двоеточия. В `HHMM.F` дробная часть относится к секунде. |
| `HH:MM AM`<br>`HH:MM:SS AM`<br>`HH:MM:SS.F AM`<br>`HHMM AM`<br>`HHMM.F AM`<br>`HHMMSS AM`<br>`HHMMSS.F AM` | `12:59 AM`<br>`02:59:59 PM` | С AM или PM |
| `HH:MMAM`<br>`HH:MM:SSAM`<br>`HH:MM:SS.FAM` | `02:59PM` | AM или PM без пробела |

## Часовой пояс {: #time_zone }

| Формат | Пример | Примечание |
| --- | --- | --- |
| `Z`<br>`±HH`<br>`±HMM`<br>`±HHMM`<br>`±HH:MM`<br>`±HH:MM:SS` | `Z`<br>`+03`<br>`+530`<br>`-0330`<br>`+05:45`<br>`-03:30:00` | UTC и числовое смещение |
| `±HH DST`<br>`±HMM DST`<br>`±HHMM DST`<br>`±HH:MM DST`<br>`±HH:MM:SS DST` | `+02 DST` | Числовое смещение с DST |
| `ABBR`<br>`ABBR DST`<br>`IANA` | `MSK`<br>`CET DST`<br>`Europe/Moscow` | Аббревиатура или название из базы IANA. `ABBR DST` — кроме `ABBR` летнего времени, например `PDT`, и `ABBR` с переменным смещением, например `MSK`. |

### Часовой пояс POSIX {: #posix_time_zone }

Часовой пояс в форме переменной [TZ] из POSIX.
Положительное смещение означает запад: `UTC+3` — это `-03:00`.

| Формат | Пример | Примечание |
| --- | --- | --- |
| `NAME±HH`<br>`NAME±HH:MM`<br>`NAME±HH:MM:SS` | `UTC+3`<br>`UTC-05:30` | Без летнего времени |
| `NAME±HHNAME`<br>`NAME±HHNAME±HH`<br>`NAME±HHNAME±HH:MM`<br>`NAME±HHNAME±HH:MM:SS` | `EST+5EDT` | С летним временем: основное смещение в часах |
| `NAME±HH:MMNAME`<br>`NAME±HH:MMNAME±HH`<br>`NAME±HH:MMNAME±HH:MM`<br>`NAME±HH:MMNAME±HH:MM:SS` | `NST3:30NDT2:30` | С летним временем: основное смещение в часах и минутах |
| `NAME±HH:MM:SSNAME`<br>`NAME±HH:MM:SSNAME±HH`<br>`NAME±HH:MM:SSNAME±HH:MM`<br>`NAME±HH:MM:SSNAME±HH:MM:SS` | `XST+3:30:15XDT+2:30:15` | С летним временем: основное смещение с секундами |

`NAME` — любое слово из латинских букв. Первое `NAME` перед `+` или
цифрой не может быть `Mon`, `Dy`, `AM` или одним из слов `AD`, `BC`,
`at`, `on`, `DST`, `J`, `JD`, `Julian`, `T`, `D`, `H`, `M`, `MM`, `S`,
`Y`, `allballs`, `epoch`, `infinity`, `now`, `today`, `tomorrow`,
`yesterday`, `dow`, `doy`, `isodow`, `isoyear`.

После `NAME` знак `+` можно опустить. Ограничения правил перехода на летнее
время описаны в разделе о [стандартах](#standards).

## Дата, время и часовой пояс {: #datetime }

`<DATE>`, `<TIME>` и `<TZ>` — записи из разделов [дата](#date),
[время](#time) и [часовой пояс](#time_zone) соответственно.

| Формат | Пример | Примечание |
| --- | --- | --- |
| `<DATE>`<br>`<DATE> <TZ>`<br>`<DATE> <TIME>`<br>`<DATE> <TIME> <TZ>`<br>`<DATE> <TZ> <TIME>` | `2025-10-18 02:59:59 MSK` | Дата с необязательными временем и часовым поясом |
| `<DATE> HH:MM<TZ>`<br>`<DATE> HH:MM:SS<TZ>`<br>`<DATE> HH:MM:SS.F<TZ>`<br>`<DATE> MM:SS.F<TZ>` | `2025-10-18 02:59:59.5Z` | Часовой пояс вплотную ко времени с двоеточием |
| `<DATE> HHMM<TZ>`<br>`<DATE> HHMM.F<TZ>`<br>`<DATE> HHMMSS<TZ>`<br>`<DATE> HHMMSS.F<TZ>` | `2025-10-18 025959+03:00` | Часовой пояс вплотную ко времени без двоеточия. `<DATE> HHMM<TZ>` и `<DATE> HHMMSS<TZ>` — кроме `<TZ>` вида `-HH:MM` и `-HH:MM:SS`. |
| `<DATE> at <TIME>`<br>`<DATE> at <TIME> <TZ>`<br>`on <DATE>`<br>`on <DATE> <TIME>`<br>`on <DATE> <TIME> <TZ>`<br>`on <DATE> at <TIME>` | `on Oct 18 2025 at 2:59 PM` | Со словами at и on |
| `<DATE> allballs`<br>`epoch` | `2025-10-18 allballs`<br>`epoch` | Специальные значения |

### С буквой T {: #iso_t }

До и после `T` можно поставить пробел. Без пробела перед `T` `<DATE>` —
кроме дат с `Mon` и разделителями `-`, `/`, `.`, а также `YYYY DD Mon`.

| Формат | Пример | Примечание |
| --- | --- | --- |
| `<DATE>T<TIME>`<br>`<DATE>T<TIME> <TZ>` | `2025-10-18T02:59:59 Europe/Moscow` | Без часового пояса или с пробелом перед ним |
| `<DATE>THH:MM<TZ>`<br>`<DATE>THH:MM:SS<TZ>`<br>`<DATE>THH:MM:SS.F<TZ>`<br>`<DATE>TMM:SS.F<TZ>` | `2025-10-18T02:59:59.52Z` | Часовой пояс вплотную ко времени с двоеточием |
| `<DATE>THHMM<TZ>`<br>`<DATE>THHMM.F<TZ>`<br>`<DATE>THHMMSS<TZ>`<br>`<DATE>THHMMSS.F<TZ>` | `20251018T025959Z` | Часовой пояс вплотную ко времени без двоеточия. `<DATE>THHMM<TZ>` и `<DATE>THHMMSS<TZ>` — кроме `<TZ>` вида `-HH:MM` и `-HH:MM:SS`. |

### С днем недели {: #weekday }

| Формат | Пример | Примечание |
| --- | --- | --- |
| `<DATE> Dy`<br>`<DATE>, Dy`<br>`<DATE> Dy <TIME>`<br>`<DATE> <TIME> Dy`<br>`<DATE> <TIME> <TZ> Dy` | `2025-10-18 Sat 02:59:59` | После даты |
| `Dy <DATE>`<br>`Dy, <DATE>`<br>`Dy <DATE> <TIME>`<br>`Dy, <DATE> <TIME>`<br>`Dy <DATE> <TIME> <TZ>`<br>`Dy, <DATE> <TIME> <TZ>` | `Sat Oct 18 2025` | Перед датой |

После `Dy` `<DATE>` — без разделителей `-`, `/`, `.`.
День недели не сверяется с датой.

### Время внутри даты {: #time_inside_date }

`<TIME>` — любой [формат времени](#time), кроме `HHMM` и
`HHMMSS`, с `AM` или без. Его можно поставить между любыми частями
`<DATE>`, разделенными пробелом, кроме юлианского дня.

| Формат | Пример | Примечание |
| --- | --- | --- |
| `Mon DD <TIME> YYYY`<br>`Dy Mon DD <TIME> YYYY`<br>`DD Mon <TIME> YYYY` | `Oct 18 02:59:59 2025` | Без часового пояса |
| `Mon DD <TIME> YYYY <TZ>`<br>`Dy Mon DD <TIME> YYYY <TZ>`<br>`DD Mon <TIME> YYYY <TZ>`<br>`Dy DD Mon <TIME> YYYY <TZ>` | `Oct 18 02:59:59 2025 PST` | Часовой пояс после года |
| `Mon DD <TIME> <TZ> YYYY`<br>`Dy Mon DD <TIME> <TZ> YYYY`<br>`DD Mon <TIME> <TZ> YYYY` | `Sat Oct 18 02:59:59 MSK 2025` | Часовой пояс перед годом |

### До нашей эры {: #bc }

`<DATE>` — кроме юлианского дня, с которым `BC` и `AD` не учитываются.
Год `YY` вместе с `BC` не дополняется до четырех цифр.

| Формат | Пример | Примечание |
| --- | --- | --- |
| `<DATE> BC`<br>`<DATE> BC <TIME>`<br>`<DATE> <TIME> BC`<br>`<DATE> <TIME> <TZ> BC` | `0044-03-15 BC` | До нашей эры: BC |
| `<DATE> AD`<br>`<DATE> AD <TIME>`<br>`<DATE> <TIME> AD`<br>`<DATE> <TIME> <TZ> AD` | `2025-10-18 AD` | Наша эра: AD |

### Юлианский день с долей суток {: #julian_fraction }

| Формат | Пример | Примечание |
| --- | --- | --- |
| `JN.F`<br>`J N.F`<br>`JDN.F`<br>`JD N.F`<br>`JulianN.F`<br>`Julian N.F` | `J2460967.5` | Без часового пояса |
| `JN.F <TZ>`<br>`J N.F <TZ>`<br>`JDN.F <TZ>`<br>`JD N.F <TZ>`<br>`JulianN.F <TZ>`<br>`Julian N.F <TZ>` | `J2460967.5 MSK` | С часовым поясом |

## Обозначения и общие правила {: #notation }

- `YYYY` — год, 3–4 цифры
- `YY` — год, 1–2 цифры: `00`–`69` — 2000–2069, `70`–`99` — 1970–1999
- `MM` — месяц в дате, минуты во времени
- `DD` — день месяца
- `DDD` — номер дня в году, 3 цифры
- `Mon` — месяц: `January`…`December`, `Jan`…`Dec`, `Sept`
- `Dy` — день недели: `Sunday`…`Saturday`, `Sun`…`Sat`, `Tues`, `Weds`,
  `Thur`, `Thurs`; не сверяется с датой
- `HH` — часы
- `SS` — секунды
- `F` — дробная часть, любое число цифр
- `N` — номер юлианского дня
- `AM` — `AM` или `PM`
- `ABBR` — аббревиатура часового пояса из набора [Default] PostgreSQL
- `IANA` — название часового пояса из [базы IANA][iana-tz]
- `NAME` — название в [часовом поясе POSIX](#posix_time_zone)
- `±` — `+` или `-`; после `NAME` знак `+` можно опустить
- `<DATE>` — любой [формат даты](#date)
- `<TIME>` — любой [формат времени](#time)
- `<TZ>` — любой [формат часового пояса](#time_zone)

`YY`, `MM`, `DD`, `HH` и `SS` — одна или две цифры, а в записи без
разделителей (`YYMMDD`, `YYYYMMDD`, `HHMMSS`, `±HHMM`) — ровно две. Регистр букв
не учитывается. После знака `±` можно поставить пробел. Вместо
остальных пробелов можно поставить любой знак препинания ASCII, кроме
`+`, `-`, `.`, `/`, `:` и `_`. Пробел между числом и словом можно не
ставить, кроме пробела после `<TZ>` и после `<DATE>` с `Mon` и
разделителями `-`, `/`, `.`.

Слова `Dy`, `AM`, `BC`, `AD`, `at`, `on` и `ABBR` можно поставить в
начало или конец строки или между любыми ее частями, разделенными
пробелом. Перед `<DATE>` с разделителями `-`, `/`, `.` можно поставить
только `at` и `on`.

## Стандарты {: #standards }

Ни в одном формате не поддерживаются моменты позже
`9999-12-31 23:59:59` UTC.

### ISO 8601-1 {: #iso_8601 }

Частичная поддержка [представления даты и времени][ISO 8601-1].
Не поддерживаются:

- даты по неделям: `2025-W42-6`, `2025W426`
- порядковая дата `YYYYDDD`
- неполные дата и время: `2025-10`, `2025`, `2025-10-18T02`
- дата и время без разделителя: `20251018025959`
- десятичная запятая: `02:59:59,5`
- доли часа и минуты: `HH:MM.F` читается как `MM:SS.F`,
  а `F` в `HHMM.F` — как доля секунды
- расширенный, нулевой и отрицательный год: `+002025-10-18`,
  `0000-01-01`, `-0044-03-15`
- смещения от `±16:00`
- знак `−` (U+2212) вместо `-`
- интервалы, длительности и повторения

### RFC 3339 {: #rfc_3339 }

Частичная поддержка [даты и времени в интернете][RFC 3339].
Не поддерживаются:

- смещения от `±16:00` до `±23:59`
- год `0000`
- секунда `60` — она переносится в начало следующей минуты

### RFC 5322 {: #rfc_5322 }

[Формат интернет-сообщений][RFC 5322] (ранее RFC 822 и RFC 2822),
поле `date-time`. Поддерживается частично. Не поддерживаются:

- смещения от `±1600`
- комментарии: `(MSK)`
- пробелы вокруг `:`
- военные однобуквенные часовые пояса, кроме `Z`
- двузначный год `50`–`69` — он читается как 2050–2069, а не 1950–1969
- трехзначный год — к нему не прибавляется 1900

### RFC 9110 {: #rfc_9110 }

[Семантика HTTP][RFC 9110] (ранее RFC 1123, RFC 2616 и RFC 7231),
поле `HTTP-date`. Поддерживается частично. Не поддерживается `rfc850-date`:
`Sunday, 06-Nov-94 08:49:37 GMT`.

### RFC 5545 {: #rfc_5545 }

[Формат календарных данных iCalendar][RFC 5545], значения `DATE` и
`DATE-TIME`. Поддерживаются частично: время без `Z` считается временем UTC.

### SQL и C {: #sql_c }

Полная поддержка следующих стандартов:

- [ISO/IEC 9075-2 — язык баз данных SQL][ISO/IEC 9075-2]: строки литералов
  `DATE` и `TIMESTAMP`
- [ISO/IEC 9899 — язык программирования C][ISO/IEC 9899]: вывод функций
  `asctime` и `ctime`

### POSIX {: #posix }

Частичная поддержка.

Вывод утилиты [`date`][POSIX] по умолчанию — только если часовой пояс
задан как `ABBR` или смещение.

Переменная окружения [`TZ`][TZ], форма `std offset [dst [offset]]` —
см. описание [часового пояса POSIX](#posix_time_zone).
Положительное смещение означает запад: `UTC+3` — это `-03:00`.
Не поддерживаются:

- правила перехода на летнее время: `EST5EDT,M3.2.0,M11.1.0`;
  без них действуют правила США
- названия в угловых скобках: `<+03>-3`

### RFC 9557 и X.680 {: #unsupported_standards }

Не поддерживаются:

- [Дата и время в интернете с дополнительной информацией][RFC 9557]:
  суффиксы в квадратных скобках, например
  `2025-10-18T02:59:59+03:00[Europe/Moscow]`
- [Нотация ASN.1][X.680]: `UTCTime` и `GeneralizedTime`, например
  `251018025959Z`, `20251018025959Z`

</div>

<style>
.datetime-formats code {
    display: inline-block;
    max-width: 100%;
    overflow-wrap: anywhere;
}

.datetime-formats .md-typeset__scrollwrap {
    margin: 1em 0;
}

.datetime-formats .md-typeset__table {
    display: block;
    padding: 0;
}

.md-typeset .datetime-formats table {
    width: 100%;
    table-layout: fixed;
    font-size: inherit;
}

.datetime-formats th:first-child {
    width: 45%;
}

.datetime-formats th:nth-child(2) {
    width: 25%;
}

@media (max-width: 48em) {
    .md-typeset .datetime-formats table,
    .datetime-formats tbody,
    .datetime-formats tr,
    .datetime-formats td {
        display: block;
        width: 100%;
    }

    .datetime-formats thead {
        position: absolute;
        width: 1px;
        height: 1px;
        overflow: hidden;
        clip-path: inset(50%);
    }

    .md-typeset .datetime-formats table td {
        border: 0;
        padding: 0.75em 1em;
    }

    .datetime-formats tr + tr {
        border-top: 0.05rem solid var(--md-typeset-table-color);
    }

    .md-typeset .datetime-formats table td + td {
        padding-top: 0;
    }

    .datetime-formats td:nth-child(2)::before {
        content: "Пример: ";
        font-weight: bold;
    }
}
</style>

[DATETIME]: ../sql_types.md#datetime
[Default]: https://github.com/postgres/postgres/blob/REL_18_STABLE/src/timezone/tznames/Default
[iana-tz]: https://www.iana.org/time-zones
[ISO 8601-1]: https://www.iso.org/standard/70907.html
[RFC 3339]: https://datatracker.ietf.org/doc/html/rfc3339
[RFC 5322]: https://datatracker.ietf.org/doc/html/rfc5322#section-3.3
[RFC 9110]: https://datatracker.ietf.org/doc/html/rfc9110#section-5.6.7
[RFC 5545]: https://datatracker.ietf.org/doc/html/rfc5545#section-3.3.5
[ISO/IEC 9075-2]: https://www.iso.org/standard/76584.html
[ISO/IEC 9899]: https://www.iso.org/standard/82075.html
[POSIX]: https://pubs.opengroup.org/onlinepubs/9799919799/utilities/date.html
[TZ]: https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/V1_chap08.html#tag_08_03
[RFC 9557]: https://datatracker.ietf.org/doc/html/rfc9557
[X.680]: https://www.itu.int/rec/T-REC-X.680
