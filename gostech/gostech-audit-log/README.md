# Отправка аудит логов

Парсит аудит лог и отправляет его на указанный урл.

## Запуск через файловый лог

```shell
./target/debug/gostech-audit-log --url https://example.com --filename audit.log --type file
```

## Запуск через пайп

```shell
cat audit.log | ./target/debug/gostech-audit-log --url https://example.com --type pipe
```
