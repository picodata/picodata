# Рабочие файлы инстанса {: #instance_runtime_files }

Picodata хранит в рабочей директории инстанса [`--instance-dir`] следующие
файлы:

- `*.snap` — снапшоты локальной БД на движке memtx
- `*.xlog` — журнал упреждающей записи (Write-ahead log, WAL) c
  инкрементными изменениями локальной БД на движке memtx
- `*.vylog` — инкрементальные изменения БД для таблиц, использующих
  движок vinyl

Перечисленные файлы в совокупности содержат все данные, хранящиеся в
локальной БД. Сохранность этих файлов является залогом персистентного
хранения данных.

[`--instance-dir`]: ../reference/cli.md#run_instance_dir

См. также:

- [Резервное копирование и восстановление](../admin/backup_and_restore.md)
- [Глоссарий — WAL](../overview/glossary.md#wal)
- [Глоссарий — Персистентность](../overview/glossary.md#persistence)
- [Глоссарий — Движок хранения](../overview/glossary.md#db_engine)
- [Глоссарий — Снапшоты](../overview/glossary.md#snapshot)
