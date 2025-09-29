# PICO_TIER_NAME

Скалярная функция `pico_tier_name` позволяет узнать имя [тира][tier], к
которому относится текущий инстанс, предоставив текстовое значение его [UUID].

## Синтаксис {: #syntax }

![PICO_TIER_NAME](../../images/ebnf/pico_tier_name.svg)

## Примеры использования {: #using_examples }

```sql title="Универсальный пример"
SELECT pico_tier_name(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_tier_name('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет имя [тира][tier], участником которого является текущий инстанс.

[tier]: ../../overview/glossary.md#tier
[UUID]: ../../reference/sql_types.md#uuid
