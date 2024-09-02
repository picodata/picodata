# Запуск Picodata

В данном разделе приведена информация по запуску отдельного инстанса
Picodata на физическом оборудовании или в виртуальной среде.

[Инстанс](../overview/glossary.md#instance) — экземпляр приложения
Picodata, из которых состоит кластер. Picodata может создать кластер,
состоящий всего из одного экземпляра/инстанса.

## Минимальный вариант запуска {: #simple_run }

В самом простом случае запуск инстанса сводится к выполнению команды
 `picodata run` без каких-либо параметров:

```shell
picodata run
```

Будет запущен кластер, состоящий из одного инстанса. Все параметры будут
стандартны, и, скорее всего, вы захотите их переопределить.

Читайте далее:

- [Подключение и работа в консоли](../tutorial/connecting.md)

## Запуск нескольких инстансов {: #run_multiple_instances }

Для того чтобы запустить несколько инстансов на одном сервере,
потребуется задать дополнительные параметры для каждого из них:

- рабочую директорию ([--data-dir])
- сетевой адрес ([--listen])
- адрес одного или нескольких соседних инстансов ([--peer])

Полный перечень возможных параметров запуска и их
описание содержатся в разделе [Аргументы командной строки], а также в
выводе команды `picodata run --help`.

[Аргументы командной строки]: ../reference/cli.md

Чтобы запустить два инстанса, которые объединятся в кластер,
выполните в двух соседних терминалах следующие команды:

```shell
picodata run --data-dir ./data/i1 --listen 127.0.0.1:3301
```

```shell
picodata run --data-dir ./data/i2 --listen 127.0.0.1:3302 --peer 127.0.0.1:3301
```

Обратите внимание на различия в запуске инстансов:

- каждый инстанс использует свою рабочую директорию и сетевой адрес
- в отсутствие параметра `--peer` запуск инстанса приводит к созданию
  нового кластера. Чтобы инстансы добавлялись в уже существующий кластер
  в этом параметре передается адрес первого инстанса

[--data-dir]: ../reference/cli.md#run_data_dir
[--listen]: ../reference/cli.md#run_listen
[--peer]: ../reference/cli.md#run_peer

Читайте далее:

- [Создание кластера](../tutorial/deploy.md)

## Запуск с помощью Docker Compose {: #run_docker_compose }

Мы поддерживаем запуск Picodata при помощи инструментария Docker Compose
и поставляем [yaml-файл][docker-compose.yml] для запуска тестового
кластера из 4-х инстансов. Каждый инстанс работает в отдельном
контейнере Docker. Этот способ удобен также тем, что позволяет
попробовать наше ПО в действии без необходимости [установки
пакетов][available_packages] или [сборки из исходного
кода][installing_from_sources].

[docker-compose.yml]: https://git.picodata.io/picodata/picodata/picodata/-/blob/master/helm/docker-compose.yml?ref_type=heads
[available_packages]: install.md#available_packages
[installing_from_sources]: install.md#installing_from_sources

Для развертывания тестового кластера данным способом:

- убедитесь, что у вас установлены Docker, Docker Compose, а также что
  системная служба `docker` запущена
- скачайте файл [docker-compose.yml] и откройте терминал в директории,
  в которой он находится
- создайте директорию для рабочих файлов проекта: `mkdir pico`
- задайте путь к Docker-репозиторию Picodata:
```shell
export REGISTRY=docker-public.binary.picodata.io
```
- запустите контейнеры проекта: `docker-compose up -d`

Для подключения к административной консоли тестового кластера
используйте команду:

```shell
picodata connect admin@localhost:13301
```

Пароль администратора можно [посмотреть][pw] в yaml-файле проекта.

[pw]: https://git.picodata.io/picodata/picodata/picodata/-/blame/master/helm/docker-compose.yml#L17

Альтернативный вариант — подключение через файл сокета:

```shell
picodata admin pico/data/picodata-1-1/admin.sock
```

См. также:

- [Docker Compose overview](https://docs.docker.com/compose/)


## Безопасный запуск {: #secure_run }

<!-- WARNING: "‑" below are non-breaking hyphen &#8209; -->

Для обеспечения мер безопасности рекомендуется организовать хранение
пароля для внутреннего системного пользователя `pico_service` в
отдельном файле. При запуске инстанса путь к этому файлу передайте в
параметре [‑‑service‑password‑file][pwdfile].

Дополнительно, в опции [‑‑audit][audit] явно укажите указать способ
вывода [журнала аудита](../tutorial/audit_log.md).

Также рекомендуется использовать опцию [‑‑shredding][shredding], которая
обеспечивает безопасное удаление рабочих файлов.

Пример команд, реализующих безопасный запуск:

```shell
echo "shAreD_s3cr3t" > secret.txt
chmod 600 secret.txt
picodata run --service-password-file secret.txt --audit audit.log --shredding
```

[pwdfile]: ../reference/cli.md#run_service_password_file
[audit]: ../reference/cli.md#run_audit
[shredding]: ../reference/cli.md#run_shredding

## Безопасное завершение работы {: #secure_stop }

Для безопасного завершения работы инстанса нажмите сочетание
++ctrl+c++ в консоли, в которой он был запущен. После этого процесс
инстанса будет корректно завершен.
