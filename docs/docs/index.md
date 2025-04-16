---
search:
  exclude: true
---

# Добро пожаловать на портал документации Picodata

Picodata — это распределенная система промышленного уровня для
управления базами данных с возможностью расширения функциональности за
счет плагинов. Исходный код Picodata открыт и доступен как в нашем
[основном репозитории](https://git.picodata.io/core/picodata), так и в
[зеркале на GitHub](https://github.com/picodata/picodata).

Программное обеспечение Picodata реализует хранение структурированных и
неструктурированных данных, транзакционное управление данными, язык
запросов SQL, а также поддержку плагинов на языке Rust.

На этом портале представлена документация по Picodata:

### Ознакомление с ПО Picodata {: #overview }

* [Общее описание продукта](overview/description.md)
* [Преимущества использования Picodata](overview/benefits.md)
* [Глоссарий](overview/glossary.md)
* [Обратная связь и получение помощи](overview/feedback.md)
* [Лицензирование](overview/licensing.md)
* [Политика версионирования](overview/versioning.md)

### Инструкции и руководства {: #tutorial }

* [Установка Picodata](tutorial/install.md)

#### Запуск и развертывание {: #run_deploy }

* [Запуск Picodata](tutorial/run.md)
* [Создание кластера](tutorial/deploy.md)
* [Добавление узлов](tutorial/node_add.md)
* [Удаление узлов](tutorial/node_expel.md)

#### Начало работы {: #getting_started }

* [Подключение и работа в консоли](tutorial/connecting.md)
* [Подключение через DBeaver](tutorial/dbeaver.md)
* [Работа с данными SQL](tutorial/sql_examples.md)
* [Работа в веб-интерфейсе](tutorial/webui.md)

#### Разработка приложений {: #app_development }

* [Создание плагина](tutorial/create_plugin.md)
* [Управление плагинами](tutorial/plugins.md)
* [Использование внешних коннекторов к Picodata](connectors_index.md)

### Администрирование кластера {: #administration }

* [Развертывание кластера через Ansible](admin/deploy_ansible.md)
* [Picodata в Kubernetes](admin/deploy_kubernetes.md)
* [Управление кластером в промышленной среде с ограниченными привилегиями](admin/cluster_mgmt.md)
* [Конфигурирование](admin/configure.md)
* [Резервное копирование и восстановление](admin/backup_and_restore.md)
* [Мониторинг](admin/monitoring.md)
* [Управление доступом](admin/access_control.md)
* [Аутентификация с помощью LDAP/LDAPS](admin/ldap.md)
* [Включение протокола SSL](admin/ssl.md)
* [Использование журнала аудита](admin/audit_log.md)
* [Рекомендации по сайзингу](admin/sizing.md)
* [Настройка Systemd](admin/systemd.md)
* [Устранение неполадок](admin/troubleshooting.md)

### Справочные материалы {: #reference }

* [Язык SQL](sql_index.md)
* [Аргументы командной строки](reference/cli.md)
* [Файл конфигурации](reference/config.md)
* [Регистрируемые события безопасности](reference/audit_events.md)
* [Параметры конфигурации СУБД](reference/db_config.md)
* [Переменные, используемые в роли Ansible](reference/ansible_variables.md)
* [Справочник метрик](reference/metrics.md)
* [Справочник настроек](reference/settings.md)
* [Ограничения](reference/limitations.md)

### Архитектура {: #architecture }

* [Распределенный SQL](architecture/distributed_sql.md)
* [Алгоритм discovery](architecture/discovery.md)
* [Жизненный цикл инстанса](architecture/instance_lifecycle.md)
* [Рабочие файлы инстанса](architecture/instance_runtime_files.md)
* [Управление топологией](architecture/topology_management.md)
* [Raft и отказоустойчивость](architecture/raft_failover.md)
* [Описание системных таблиц](architecture/system_tables.md)
* [Интерфейс RPC API](architecture/rpc_api.md)
* [Файберы, потоки и многозадачность](architecture/fibers.md)
* [Механизм плагинов](architecture/plugins.md)

### Плагины {: #plugins }

* [Argus](plugins/argus.md)
* [Kirovets](plugins/kirovets.md)
* [Radix](plugins/radix.md)
* [Silver](plugins/silver.md)
* [Sirin](plugins/sirin.md)
* [Synapse](plugins/synapse.md)
* [Ouroboros](plugins/ouroboros.md)
* [Внешний модуль аудита](plugins/gostech_audit_log.md)

### Обеспечение безопасности {: #security }

* [Работа в защищенной ОС](security/os.md)
* [Ограничение программной среды](security/runtime.md)
* [Журнал аудита в защищенной ОС](security/audit_log.md)
* [Контроль целостности](security/integrity.md)

На данном портале представлена техническая документация программного
продукта Picodata. Информация о выгодах сотрудничества с компанией
Picodata, корпоративных решениях и услугах, новостях, событиях находится
на сайте [picodata.io](https://picodata.io).

<a style="display: none" href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fdocs.picodata.io%2Fpicodata%2F&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false"/></a>
