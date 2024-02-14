# Политика версионирования

Picodata использует формат обозначения версий, основанный на семантике
[CalVer](https://calver.org/){:target="_blank"} с отдельными элементами
[SemVer](https://semver.org/lang/ru/){:target="_blank"}. Номер версии
имеет следующий формат:

```plain
YY.MINOR.PATCH[-N-gHASH]
```

Пример:

```plain
24.1.1-1-g8065ebc2
```

где:

- `YY` — год выпуска версии
- `MINOR` — порядковый номер выпуска в году
- `PATCH` — порядковый номер исправления
- `N` — количество коммитов с последнего git-тега (для отладочных версий)
- `HASH` — хеш коммита (для отладочных версий)

Разработка новых функций и исправлений ведется в ветке `master` нашего
[Git-репозитория](https://git.picodata.io/picodata/picodata/picodata/-/tree/master){:target="_blank"}.

Мы придерживаемся следующего распорядка при разработке:

- Версия из двух чисел `YY.MINOR` определяет набор функций,
  запланированных на данную серию выпусков
- Версия из трех чисел `YY.MINOR.PATCH` описывает конкретный выпуск с
  исправлениями
- Версия в полном формате `YY.MINOR.PATCH-N-gHASH` описывает отладочную
  сборку на конкретном git-коммите

Текущая политика версионирования применяется с версии 24.1.

Cм. также:

- [Gitlab — Changelog](https://git.picodata.io/picodata/picodata/picodata/-/blob/master/CHANGELOG.md)
- [Gitlab — Tags](https://git.picodata.io/picodata/picodata/picodata/-/tags)
