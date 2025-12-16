# Установка Picodata

Данный раздел содержит сведения об установке Picodata на локальный
компьютер.

## Установка готовых пакетов {: #available_packages }

Picodata поставляется для поддерживаемых операционных систем и
предназначена для архитектуры x86_64 (в случае с macOS также
поддерживается Apple Silicon). Для Linux мы поддерживаем собственные
репозитории с готовыми пакетами для Fedora 40-42, RHEL 8, РЕД ОС 7.3
"Муром", Astra Linux 1.7 и 1.8 SE, Debian 11 и 12, Ubuntu 20.04, 22.04 и
24.04, Alt Linux p10 и ROSA Chrome 2021.1. Внутри пакетов находится
статически слинкованная версия исполняемого файла `picodata`. Более
подробная информация об установке приведена на сайте
[https://picodata.io/download](https://picodata.io/download/).

## Установка из исходного кода {: #installing_from_sources }

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

Далее приведены команды для установки остальных зависимостей под разные ОС.

#### RHEL 8/9 и деривативы, Fedora 41-43 {: #rhel_fedora }

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

#### Ubuntu 22.04 и 24.04 {: #ubuntu_lts }

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

#### Alt Server p10 {: #alt_server_p10 }

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

#### macOS {: #macos }

Сборка под macOS почти не отличается от таковой в Linux. Потребуется
macOS 10.15 Catalina, либо более новая версия (11+).

Для начала следует установить актуальные версии [Rust и
Cargo](https://rustup.rs).

Для работы веб-интерфейса Picodata следует установить дополнительно
NodeJS и Yarn при помощи пакетного менеджера [Brew](https://brew.sh).

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
