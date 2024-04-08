# Установка Picodata

Данный раздел содержит сведения об установке Picodata на локальный
компьютер.

## Установка готовых пакетов {: #available_packages }

Picodata поставляется для поддерживаемых операционных систем и
предназначена для архитектуры x86_64 (в случае с macOS также
поддерживается Apple Silicon). Для Linux мы поддерживаем собственные
репозитории с готовыми пакетами для RHEL/CentOS 7-8 И Rocky Linux 8, РЕД
ОС 7.3 "Муром", Astra Linux 1.7 SE, Debian 11, Ubuntu 20.04, 22.04, Alt
Linux p10 и ROSA Chrome 2021.1. Внутри пакетов находится статически
слинкованная версия исполняемого файла `picodata`. Более подробная
информация об установке приведена на сайте
[https://picodata.io/download](https://picodata.io/download/).

## Установка из исходного кода {: #installing_from_sources }

### Необходимые инструменты {: #prerequisites }

<!--
IMPORTANT
Указанная здесь версия rust должна быть согласована с Cargo.toml, см:
https://git.picodata.io/picodata/picodata/picodata/-/blob/master/Cargo.toml#L6
-->

- [Rust и Cargo](http://www.rustup.rs) 1.76 или новее
- cmake 3.16 или новее
- gcc, g++
- libstdc++-static
- NodeJS и Yarn (для сборки с веб-интерфейсом)

Установка Rust и Cargo универсальна для всех поддерживаемых ОС:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

Далее приведены команды для установки остальных зависимостей под разные ОС.

#### CentOS 8, Fedora 37-40 {: #centos_fedora }

Только для CentOS 8:

```bash
sudo dnf config-manager --set-enabled powertools
```

Установка общих зависимостей для сборки:

```bash
sudo dnf in -y gcc gcc-c++ make perl automake libtool cmake git patch libstdc++-static
```

Установка NodeJS и Yarn (для веб-интерфейса):

```bash
curl -sL https://rpm.nodesource.com/setup_lts.x | sudo bash -
curl -sL https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo
sudo dnf install yarn nodejs
```

#### Ubuntu 22.04 {: #ubuntu_22.04 }

Установка общих зависимостей для сборки:

```bash
sudo apt-get install build-essential git cmake autoconf libtool -y
```

Установка NodeJS и Yarn (для веб-интерфейса):

```bash
curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
sudo apt install yarn npm -y
sudo curl --compressed -o- -L https://yarnpkg.com/install.sh | bash
```

#### Alt Server p10 {: #alt_server_p10 }

Установка общих зависимостей для сборки:

```bash
su -
apt-get install -y gcc gcc-c++ cmake git patch libstdc++10-devel-static libgomp10-devel-static
```


Установка NodeJS и Yarn (для веб-интерфейса):

```bash
su -
apt-get install -y node yarn
```
<!--
```bash
cargo build --features dynamic_build
```

Зависимости:

- libcurl
- libgomp
- libldap
- libsasl
- libyaml
- libzstd
- ncurses
- openssl
- readline
-->

#### macOS {: #macos }

Сборка под macOS почти не отличается от таковой в Linux. Потребуется
macOS 10.15 Catalina, либо более новая версия (11+).

Для начала следует установить актуальные версии [Rust и
Cargo](https://rustup.rs).

Если планируется сборка Picodata c веб-интерфейсом, то нужно будет
установить дополнительно NodeJS и Yarn при помощи пакетного менеджера
[Brew](https://brew.sh).

Установка Brew:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Установка NodeJS и Yarn:

```
brew install node yarn
```

### Получение исходного кода {: #getting_sources }

```bash
git clone https://git.picodata.io/picodata/picodata/picodata.git
cd picodata
git submodule update --init --recursive
```

### Сборка {: #building }

Сборка Picodata только c консольным интерфейсом:

```bash
cargo build --release
```

Сборка Picodata c консольным и веб-интерфейсом:

```bash
cargo build --release --features webui
```

Исполняемый файл `picodata` появится в директории `target/release`.

### Проверка установки {: #post_install_check }

Когда программное обеспечение Picodata установлено, то можно проверить
наличие в системе основного исполняемого файла `picodata`, используя
следующую команду:

```bash
which picodata
```

Ответом на неё должно быть значение `/usr/bin/picodata`, либо — если вы
устанавливали ПО вручную с другим префиксом — иное расположение,
включенное в `$PATH`. Чтобы убедиться в работоспособности ПО, а также
посмотреть его версию, используйте следующую команду:

```bash
picodata --help
```

В состав ПО также включены юнит-тесты, позволяющие проверить
работоспособность основных функций. Юнит-тесты можно запустить следующей
командой:

```bash
picodata test
```

Пример вывода команды:

```bash
running 6 tests
test test_traft_pool ... ok
test test_storage_peers ... ok
test test_storage_state ... ok
test test_storage_log ... ok
test test_mailbox ... ok
test test_version ... ok

test result: ok. 6 passed; 0 failed; finished in 0.88s
```

## Обновление Picodata {: #update_picodata }

Обновление без потери данных поддерживается для выпусков Picodata,
принадлежащих к одной серии (с совпадающим первым числом в [номере
версии](../overview/versioning.md)). Порядок действий при обновлении:

- если Picodata была установлена из готового пакета для определенной ОС,
  то следует обновить этот пакет. При наличии подключенного репозитория
  Picodata обновленный пакет будет доставлен в рамках общего обновления
  системы (`sudo apt-get upgrade` для систем c Apt и `sudo dnf update`
  для систем с Yum/DNF). Если пакет был установлен вручную, без
  подключения репозитория Picodata, то следует воспользоваться
  системными средствами для управления индивидуальными пакетами
  (`dpkg`/`rpm`)
- если Picodata была собрана из исходного кода, то необходимо обновить
  данные локальной копии Git-репозитория (`git pull && git submodule
  update --init --recursive`), очистить данные предыдущей сборки (`cargo
  clean`) и [собрать](#building) новую версию бинарного файла picodata.
  Важно [удостовериться](#post_install_check), что после этого будет
  запускаться именно новая версия `picodata`
- после обновления следует перезапустить инстанс Picodata с прежними
  параметрами

## Удаление Picodata {: #uninstall_picodata }

Порядок действий для удаления Picodata:

- перед удалением необходимо [остановить и вывести из
  кластера](../tutorial/deploy.md#expel) все запущенные на данном хосте
  инстансы Picоdata
- для каждого инстанса требуется удалить его [рабочие
  файлы](../architecture/instance_runtime_files.md). Например: `rm -rf
  *.xlog *.vylog *.snap *.sock`
- если Picodata была установлена из готового пакета для определенной ОС,
  то следует удалить этот пакет, используя системный менеджер пакетов
  (например, `sudo apt remove picodata` или `sudo dnf remove picodata`)
- если Picodata была собрана из исходного кода, то необходимо удалить
  исполняемый файл `picodata`. Если этот файл был ранее помещен в одну
  из директорий, входящих в `$PATH`, то следует явно удалить его оттуда:
  `sudo rm -rf $(which picodata)`. После этого следует удалить
  директорию с Git-репозиторием Picodata
- после удаления следует удостовериться, что команда `which picodata`
  сигнализирует об отсутствии исполняемого файла `picodata` в `$PATH`

<!--
## Создание приложения
Для создания приложения нужно сначала придумать его имя. Например, пусть это будет `myapp`.

Запустим команду:
`picodata create-app --name myapp`

Будет соданая новая директория `myapp` с hello-world-приложением, тестами, и инструкцией по сборке и запуску в readme. Приложение — это динамическая библиотека, собранная из исходного кода на Rust.
Сборка выполняется через `cargo build`.
При запуске `cargo test`запускаются тесты.

Теперь можно менять код, собирать и тестировать.

## Структура приложения
По структуре директорий приложение похоже на типичный проект на Rust. Код находится в ./src/. Из этого кода при запуске `cargo build` собирается приложение.
В приложении обязательно должна быть специальным образом объявлена `main`-функция Picodata. Эта функция будет выполняться при запуске. Внутри этой функции при необходимости объявляются RPC handlers. В этой же функции обычно запускаются потоки (fibers) для фоновых задач.

## Развертывание и запуск приложения
Разместить приложение, динамическую библиотеку, собранную через `cargo build`, в папке на сервере, где вам удобнее, например, в `/usr/local/lib/picodata/myapp/`.

Запустить один инстанс приложения:

```
picodata run
 --app-path /usr/local/lib/picodata/myapp/
 --app myapp
 --data-dir /var/lib/picodata/myapp
 --cluster-id myapp
 --instance-id myapp1
```

Запустить остальные инстансы аналогичным образом, передавая каждому инстансу уникальные идентификаторы (`instance-id`). У несколько инстансов на одном хосте должны быть уникальные параметры `data-dir` и `listen`.
После запуска Picodata поднимет и настроит инстанс кластера, создаст на каждом инстансе глобальную Lua-таблицу `myapp`. В ней будут функции, которые можно вызывать по протоколу Tarantool, например, через `net.box call('myapp.hello_world’, {42})`.
Клиенты могут подключаться к любому инстансу и вызывать методы приложения через вызов CALL по протоколу Tarantool. Подробнее об интеграции клиентских приложений с Tarantool см. в описании [Tarantool Rust SDK](https://git.picodata.io/picodata/picodata/tarantool-module).

## Минимальный вариант кластера

Picodata может создать кластер, состоящий всего из одного экземпляра/инстанса. Обязательных параметров у него нет, что позволяет свести запуск к выполнению всего одной простой команды:

```
picodata run
```

Можно добавлять сколько угодно последующих инcтансов — все они будут подключаться к этому кластеру. Каждому инстансу следует задать отдельную рабочую директорию (параметр `--data-dir`), а также указать адрес и порт для приема соединений (параметр `--listen`) в формате `<HOST>:<PORT>`. Фактор репликации по умолчанию равен 1 — каждый инстанс образует отдельный репликасет. Если для `--listen` указать только порт, то будет использован IP-адрес по умолчанию (127.0.0.1):

```
picodata run --data-dir i1 --listen :3301
picodata run --data-dir i2 --listen :3302
picodata run --data-dir i3 --listen :3303
```

## Кластер на нескольких серверах

Выше был показан запуск Picodata на одном сервере, что удобно для тестирования и отладки, но не отражает сценариев полноценного использования кластера. Поэтому пора запустить Picodata на нескольких серверах. Предположим, что их два: `192.168.0.1` и `192.168.0.2`. Порядок запуска будет следующим:

На `192.168.0.1`:

```shell
picodata run --listen 192.168.0.1:3301
```

На `192.168.0.2`:

```shell
picodata run --listen 192.168.0.2:3301 --peer 192.168.0.1:3301
```

На что нужно обратить внимание:

Во-первых, для параметра `--listen` вместо стандартного значения `127.0.0.1` надо указать конкретный адрес. Формат адреса допускает упрощения — можно указать только хост `192.168.0.1` (порт по умолчанию `:3301`), или только порт, но для наглядности лучше использовать полный формат `<HOST>:<PORT>`.

Значение параметра `--listen` не хранится в кластерной конфигурации и может меняться при перезапуске инстанса.

Во-вторых, надо дать инстансам возможность обнаружить друг друга для того чтобы механизм [discovery](discovery.md) правильно собрал все найденные экземпляры Picodata в один кластер. Для этого в параметре `--peer` нужно указать адрес какого-либо соседнего инстанса. По умолчанию значение параметра `--peer` установлено в `127.0.0.1:3301`. Параметр `--peer` не влияет больше ни на что, кроме механизма обнаружения других инстансов.

Параметр `--advertise` используется для установки публичного IP-адреса и порта инстанса. Параметр сообщает, по какому адресу остальные инстансы должны обращаться к текущему. По умолчанию он равен `--listen`, поэтому в примере выше не упоминается. Но, например, в случае `--listen 0.0.0.0` его придется указать явно:

```shell
picodata run --listen 0.0.0.0:3301 --advertise 192.168.0.1:3301
```

Значение параметра `--advertise` анонсируется кластеру при запуске инстанса. Его можно поменять при перезапуске инстанса или в процессе его работы командой `picodata set-advertise`.
-->

См. также:

- [Запуск Picodata](run.md)
- [Аргументы командной строки Picodata](../reference/cli.md)
