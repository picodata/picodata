# Установка Picodata

Данный раздел содержит сведения об установке Picodata из готовых
пакетов, а также сборке продукта из исходного кода.

## Обзор доступных сборок {: #available_packages }

Мы предоставляем готовые пакеты с Picodata для следующих операционных систем:

- [RHEL 8-9 и совместимые с ним дистрибутивы, например Rocky Linux](#rhel8)
- [Fedora 39-43](#fedora)
- [РЕД ОС 7.3 “Муром”](#redos)
- [Astra Linux 1.7 и 1.8 SE](#astra)
- [Debian 11 "Bullseye", 12 "Bookworm" и 13 "Trixie"](#debian)
- [Ubuntu 20.04 (Focal Fossa), 22.04 (Jammy Jellyfish), 24.04 (Noble Numbat) и совместимые дистрибутивы](#ubuntu)
- [Семейство Alt Linux на основе платформы p10](#altlinux)
- [ROSA Chrome (платформа 2021.1)](#rosalinux)

Готовые пакеты можно установить как через добавление репозиториев, так и
скачав напрямую — см. [инструкции](#distro_specific_guides) для
отдельных дистрибутивов ниже. Имейте в виду, что готовые пакеты имеют в
качестве "мягкой" зависимости компонент PostgreSQL, содержащий `psql` —
рекомендуемое приложение для [подключения] к Picodata. Это означает, что
пакетный менеджер ОС может установить вместе с Picodata также и пакет с
`psql`.

[подключения]: ../tutorial/connecting.md

Picodata также развивает свой форк Tarantool, который имеет ряд
технологических отличий от стандартного Tarantool, в том числе поддержку
кластерного SQL и дополнительные Rust API. Скачать нашу версию Tarantool
вы также можете по инструкциям ниже.

Также доступны:

- [ночные сборки](#nightly_builds) мастер-веток наших продуктов
- [Docker-образы](#docker_images) с Picodata

<a name="priv"></a>

См. также:

- [Страница загрузки Picodata](https://picodata.io/download/)

!!! note "Примечание"
    Действия по настройке репозиториев Picodata и установке готовых пакетов
    следует производить в терминале с правами администратора. Авторизуйтесь
    под пользователем `root` или повысьте привилегии через `sudo -i`.

## Инструкции для дистрибутивов Linux {: #distro_specific_guides }

### RHEL 8/Rocky 8 {: #rhel8 }

Импортируйте ключ репозитория Picodata, используя следующую команду в
терминале (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
rpm --import https://download.picodata.io/tarantool-picodata/picodata.gpg.key
```

Подключите репозиторий:

```shell
dnf install https://download.picodata.io/tarantool-picodata/el/8/x86_64/picodata-release-1.1.3.0-1.el8.x86_64.rpm
```

После успешного выполнения команды в вашей системе появится
дополнительный репозиторий в `/etc/yum.repos.d/picodata.repo`.

Установите пакет Picodata, скопировав и вставив в терминал следующие
команды:

```shell
dnf clean all
dnf install -y picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
dnf install -y tarantool-picodata
```

### RHEL 9/Rocky 9 {: #rhel9 }

Создайте файл `/etc/yum.repos.d/picodata.repo`:

```shell
echo '
[picodata]
name=Picodata Yum Repo
baseurl=https://binary.picodata.io/repository/yum/el/$releasever/$basearch/RELEASE
enabled=1
gpgcheck=0'  > /etc/yum.repos.d/picodata.repo
```

Установите Picodata (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
dnf install -y picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
dnf install -y tarantool-picodata
```

### Fedora 39–43 {: #fedora }

Создайте файл `/etc/yum.repos.d/picodata.repo`:

```shell
echo '
[picodata]
name=Picodata Yum Repo
baseurl=https://binary.picodata.io/repository/yum/fedora/$releasever/$basearch/RELEASE
enabled=1
gpgcheck=0'  > /etc/yum.repos.d/picodata.repo
```

Установите Picodata (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
dnf install -y picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
dnf install -y tarantool-picodata
```

### РЕД ОС/RED OS 7.3 “Муром” {: #redos }

Импортируйте ключ репозитория Picodata, используя следующую команду в
терминале (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
rpm --import https://download.picodata.io/tarantool-picodata/picodata.gpg.key
```

Подключите репозиторий:

```shell
dnf install https://download.picodata.io/tarantool-picodata/redos/7/x86_64/picodata-release-1.1.3.0-1.el7.x86_64.rpm
```

После успешного выполнения команды в вашей системе появится
дополнительный репозиторий в `/etc/yum.repos.d/picodata.repo`.

Установите пакет Picodata, скопировав и вставив в терминал следующие
команды:

```shell
dnf clean all
dnf install -y picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
dnf install -y tarantool-picodata
```

### Astra Linux 1.7/1.8 SE {: #astra }

Подключите репозиторий Picodata, используя следующие команды в терминале
(здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
apt-get update
apt-get install -y --no-install-recommends gpg curl apt-transport-https software-properties-common
curl -s https://download.picodata.io/tarantool-picodata/picodata.gpg.key | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/picodata.gpg --import
chmod 644 /etc/apt/trusted.gpg.d/picodata.gpg
```

Создайте файл `/etc/apt/sources.list.d/picodata.list`.

Для версии 1.7 SE:

```shell
echo '
deb [arch=amd64] https://download.picodata.io/tarantool-picodata/astra/ 1.7 main
' > /etc/apt/sources.list.d/picodata.list
```

Для версии 1.8 SE:

```shell
echo '
deb [arch=amd64] https://download.picodata.io/tarantool-picodata/astra/ 1.8 main
' > /etc/apt/sources.list.d/picodata.list
```

Обновите список источников:

```shell
apt-get update
```

Установите пакет Picodata, скопировав и вставив в терминал следующую
команду:

```shell
apt install picodata
```
`
Для того чтобы установить только пакет с нашей версией Tarantool, введите следующую команду:

```shell
apt install tarantool-picodata
```

Для работы в режиме _Замкнутой программной среды (ЗПС)_ выполните дополнительную команду:

```shell
curl -s https://download.picodata.io/tarantool-picodata/astra/picodata_pub.key -o /etc/digsig/keys/picodata_pub.key
```

После этого потребуется перезагрузка.

### Debian 11–13 {: #debian }

Подключите репозиторий Picodata, используя следующие команды в терминале (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
curl -s https://download.picodata.io/tarantool-picodata/picodata.gpg.key | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/picodata.gpg --import
chmod 644 /etc/apt/trusted.gpg.d/picodata.gpg
```

Создайте файл `/etc/apt/sources.list.d/picodata.list`:

```shell
. /etc/os-release
echo "
deb [arch=amd64] https://download.picodata.io/tarantool-picodata/debian/ $VERSION_CODENAME main
" > /etc/apt/sources.list.d/picodata.list
```

Обновите список источников:

```shell
apt-get update
```

Установите пакет Picodata, скопировав и вставив в терминал следующую команду:

```shell
apt install picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
apt install tarantool-picodata
```

### Ubuntu 20.04, 22.04, 24.04 {: #ubuntu }

Подключите репозиторий Picodata, используя следующие команды в терминале (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>):

```shell
curl -s https://download.picodata.io/tarantool-picodata/picodata.gpg.key | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/picodata.gpg --import
chmod 644 /etc/apt/trusted.gpg.d/picodata.gpg
```

Создайте файл `/etc/apt/sources.list.d/picodata.list`:

```shell
. /etc/os-release
echo "
deb [arch=amd64] https://download.picodata.io/tarantool-picodata/ubuntu/ $VERSION_CODENAME main
" > /etc/apt/sources.list.d/picodata.list
```

Обновите список источников:

```shell
apt-get update
```

Установите пакет Picodata, скопировав и вставив в терминал следующую
команду:

```shell
apt install picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
apt install tarantool-picodata
```

### Alt Linux (платформа p10) {: #altlinux }

Подключите репозиторий Picodata, используя следующие команды в терминале
(здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>)):

```shell
wget https://download.picodata.io/tarantool-picodata/altlinux/p10/picodata-release-1.0.3.0-1.p10.x86_64.rpm
apt-get install -y ./picodata-release-1.0.3.0-1.p10.x86_64.rpm
curl -s https://download.picodata.io/tarantool-picodata/picodata.gpg.key | gpg --no-default-keyring --keyring gnupg-ring:/usr/lib/alt-gpgkeys/pubring.gpg --import
```

Обновите список источников:

```shell
apt-get update
```

Установите пакет Picodata, скопировав и вставив в терминал следующую
команду:

```shell
apt-get install picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
apt-get install tarantool-picodata
```

### ROSA Chrome (платформа 2021.1) {: #rosalinux }

Подключите репозиторий Picodata, используя следующие команды в терминале (здесь и далее команды следует вводить с <a href="#priv">правами администратора</a>)):

```shell
rpm --import https://download.picodata.io/tarantool-picodata/picodata.gpg.key
dnf install https://download.picodata.io/tarantool-picodata/rosa/chrome/x86_64/picodata-release-1.1.3.0-1-rosa2021.1.x86_64.rpm
```

Установите пакет Picodata, скопировав и вставив в терминал следующую
команду:

```shell
dnf install picodata
```

Для того чтобы установить только пакет с нашей версией Tarantool,
введите следующую команду:

```shell
dnf install tarantool-picodata
```

### Ночные сборки {: #nightly_builds }

Подключите репозиторий ночных сборок с помощью следующей команды:

```shell
curl -L https://download.binary.picodata.io/tarantool-picodata/install.sh | bash
```

!!! warning "Внимание"
    Одновременное использование репозиториев Picodata
    для релизных сборок и ночных невозможно!

### Docker-образы {: #docker_images }

Доступны Docker-образы с Picodata, в том числе, distroless:

```shell
docker.binary.picodata.io/picodata:latest
docker.binary.picodata.io/picodata:<номер версии>
docker.binary.picodata.io/picodata:<номер версии>-distroless
```

Сборка из мастера:

```shell
docker.binary.picodata.io/picodata:master
docker.binary.picodata.io/picodata:master-distroless
```

Примеры команд:

```shell
docker pull docker.binary.picodata.io/picodata:25.2.1
docker pull docker.binary.picodata.io/picodata:25.1.1-distroless
docker pull docker.binary.picodata.io/picodata:master
```

Образы собраны на базе ОС Rocky Linux 8.

## Установка из исходного кода {: #installing_from_sources }

Вы можете также собрать Picodata из исходного кода в Linux и macOS,
используя инструкции ниже.

### Необходимые инструменты {: #prerequisites }

<!--
IMPORTANT
Указанная здесь версия rust должна быть согласована с Cargo.toml, см:
https://git.picodata.io/core/picodata/-/blob/master/Cargo.toml#L6
-->

- [Rust и Cargo](http://www.rustup.rs) 1.89 или новее
- cmake 3.16 или новее
- gcc, g++
- libstdc++-static
- openssl
- NodeJS и Yarn (требуются для работы веб-интерфейса Picodata)

Установка Rust и Cargo универсальна для всех поддерживаемых ОС:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

### Установка зависимостей для сборки {: #build_dependencies }

Далее приведены команды для установки остальных зависимостей под разные ОС.

??? example "RHEL 8/9 и деривативы, Fedora 39-43"
    Только для ОС, основанных на RHEL 8/9:
    ```bash
    sudo dnf config-manager --set-enabled powertools
    ```
    Установка общих зависимостей для сборки:
    ```bash
    sudo dnf in -y gcc gcc-c++ make perl automake libtool cmake git patch libstdc++-static openssl-devel
    ```
    Установка NodeJS и Yarn:
    ```bash
    curl -sL https://rpm.nodesource.com/setup_lts.x | sudo bash -
    curl -sL https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo
    sudo dnf install yarn nodejs
    ```

??? example "Ubuntu 22.04 и 24.04"
    Установка общих зависимостей для сборки:
    ```bash
    sudo apt-get install build-essential git cmake autoconf libtool curl libssl-dev pkg-config -y
    ```
    Установка NodeJS и Yarn:
    ```bash
    curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
    sudo apt install yarn npm -y
    sudo curl --compressed -o- -L https://yarnpkg.com/install.sh | bash
    ```

??? example "Alt Server p10"
    Установка общих зависимостей для сборки:
    ```bash
    su -
    apt-get install -y gcc gcc-c++ cmake git patch libstdc++-devel-static libgomp-devel-static libssl-devel-static
    ```
    Установка NodeJS и Yarn:
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

??? example "macOS"
    Сборка под macOS почти не отличается от таковой в Linux. Потребуется
    macOS 10.15 Catalina, либо более новая версия (11+).
    Для начала следует установить актуальные версии [Rust и
    Cargo](https://rustup.rs).<br>
    Для работы веб-интерфейса Picodata следует установить дополнительно
    NodeJS и Yarn при помощи пакетного менеджера [Brew](https://brew.sh).<br>
    Установка Brew:
    ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```
    Установка NodeJS и Yarn:
    ```
    brew install node yarn
    ```

### Получение исходного кода {: #getting_sources }

Загрузка с Gitlab:

```bash
git clone https://git.picodata.io/core/picodata.git --recursive
```

Загрузка с зеркала GitHub:

```bash
git clone https://github.com/picodata/picodata.git --recursive
```

### Сборка {: #building }

Используйте приведенные ниже команды для сборки Picodata. Для получения
debug-версии:

```bash
make build
```

Для получения release-версии:

```bash
make build-release-pkg
```

Исполняемый файл `picodata` появится в директории `target/debug` или
`target/release` соответственно. Далее его следует скопировать в
директорию, входящую в `$PATH`, например в `/usr/bin` или
`/usr/local/bin`.

### Проверка установки {: #post_install_check }

Когда программное обеспечение Picodata установлено, то можно проверить
наличие в системе основного исполняемого файла `picodata`, используя
следующую команду:

```bash
which picodata
```

Ответом на нее должно быть значение `/usr/bin/picodata`, либо — если вы
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

См. [Обновление кластера](../admin/cluster_update.md).

## Удаление Picodata {: #uninstall_picodata }

Порядок действий для удаления Picodata:

- перед удалением необходимо [остановить и вывести из
  кластера](../tutorial/node_expel.md#expel) все запущенные на данном хосте
  инстансы Picodata
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

См. также:

- [Запуск Picodata](run.md)
- [Аргументы командной строки Picodata](../reference/cli.md)
