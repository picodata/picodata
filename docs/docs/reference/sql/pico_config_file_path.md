# PICO_CONFIG_FILE_PATH

Скалярная функция `pico_config_file_path` возвращает путь к конфигурационному файлу,
если он был указан при запуске инстанса. Разрешено использовать только в
проекциях.

Если конфигурационный файл не был указан, функция возвращает `NULL`.

## Синтаксис {: #syntax }

![PICO_CONFIG_FILE_PATH](../../images/ebnf/pico_config_file_path.svg)

## Примеры использования {: #using_examples }

```sql
SELECT pico_config_file_path();
```

вернёт путь к использованному конфигурационному файлу или `NULL`.
