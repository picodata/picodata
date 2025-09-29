# PICO_CONFIG_FILE_PATH

Скалярная функция `pico_config_file_path` позволяет узнать полный путь к файлу
[конфигурации] инстанса, предоставив текстовое значение его [UUID]. Функция
вернет `NULL` если при запуске инстанса не использовался файл конфигурации.

Разрешено использовать только в проекциях.

[конфигурации]: ../config.md
[UUID]: pico_instance_uuid.md

## Синтаксис {: #syntax }

![PICO_CONFIG_FILE_PATH](../../images/ebnf/pico_config_file_path.svg)

## Примеры использования {: #using_examples }

```sql title="Универсальный пример"
SELECT pico_config_file_path(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_config_file_path('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет путь к использованному файлу конфигурации или `NULL`.
