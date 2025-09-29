# PICO_REPLICASET_NAME

Скалярная функция `pico_replicaset_name` позволяет узнать имя [репликасета][replicaset], к
которому относится текущий инстанс, предоставив текстовое значение его [UUID].

## Синтаксис {: #syntax }

![PICO_REPLICASET_NAME](../../images/ebnf/pico_replicaset_name.svg)

## Примеры использования {: #using_examples }

```sql title="Универсальный пример"
SELECT pico_replicaset_name(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_replicaset_name('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет имя [репликасета][replicaset], участником которого является текущий
инстанс.

[replicaset]: ../../overview/glossary.md#replicaset
[UUID]: ../../reference/sql_types.md#uuid
