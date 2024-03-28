# Рабочие файлы инстанса {: #instance_runtime_files }

<!-- WARNING: "‑" below are non-breaking hyphen &#8209; -->

Picodata хранит в рабочей директории инстанса [‑‑data‑dir][data_dir]
следующие файлы:

- `*.snap` — снапшоты локальной БД на движке memtx
- `*.xlog` — журнал упреждающей записи (Write-ahead log, WAL) c
  инкрементными изменениями локальной БД на движке memtx
- `*.vylog` — инкрементальные изменения БД для таблиц, использующих
  движок vinyl

Перечисленные файлы в совокупности содержат все данные, хранящиеся в
локальной БД. Сохранность этих файлов является залогом персистентного
хранения данных.

[data_dir]: ../reference/cli.md#run_data_dir

См. также:

- [Резервное копирование](../tutorial/backup.md)
- [Глоссарий — Персистентность](../overview/glossary.md#persistence)
- [Глоссарий — Движок хранения](../overview/glossary.md#db_engine)
- [Глоссарий — Снапшоты](../overview/glossary.md#snapshot)
