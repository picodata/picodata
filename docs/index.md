# Добро пожаловать на портал документации Picodata

Picodata — это распределенная система промышленного уровня для
управления базами данных с возможностью расширения функциональности за
счет плагинов. Исходный код Picodata
[открыт](https://git.picodata.io/picodata/picodata).
Программное обеспечение Picodata реализует хранение структурированных и
неструктурированных данных, транзакционное управление данными, язык
запросов SQL, а также поддержку плагинов на языке Rust.

На этом портале представлена документация по Picodata:

### Ознакомление с ПО Picodata {: #overview }

* [Общее описание продукта](overview/description.md)
* [Преимущества использования Picodata](overview/benefits.md)
* [Глоссарий](overview/glossary.md)
* [Обратная связь и получение помощи](overview/feedback.md)
* [Политика версионирования](overview/versioning.md)

### Инструкции и руководства {: #tutorial }

* [Установка Picodata](tutorial/install.md)
* [Запуск Picodata](tutorial/run.md)
* [Создание кластера](tutorial/deploy.md)
* [Развертывание кластера через Ansible](tutorial/deploy_ansible.md)
* [Подключение и работа в консоли](tutorial/connecting.md)
* [Работа с данными SQL](tutorial/sql_examples.md)
* [Работа в веб-интерфейсе](tutorial/webui.md)
* [Управление доступом](tutorial/access_control.md)
* [Аутентификация с помощью LDAP](tutorial/ldap.md)
* [Мониторинг кластера](tutorial/monitoring.md)
* [Использование журнала аудита](tutorial/audit_log.md)
* [Резервное копирование](tutorial/backup.md)

### Справочные материалы {: #reference }

* [Язык SQL](sql_index.md)
* [Аргументы командной строки](reference/cli.md)
* [Файл конфигурации](reference/config.md)
* [Регистрируемые события безопасности](reference/audit_events.md)

### Архитектура {: #architecture }

* [Распределенный SQL](architecture/distributed_sql.md)
* [Алгоритм discovery](architecture/discovery.md)
* [Жизненный цикл инстанса](architecture/instance_lifecycle.md)
* [Рабочие файлы инстанса](architecture/instance_runtime_files.md)
* [Управление топологией](architecture/topology_management.md)
* [Raft и отказоустойчивость](architecture/raft_failover.md)
* [Описание системных таблиц](architecture/system_tables.md)
* [Интерфейс RPC API](architecture/rpc_api.md)

### Плагины {: #plugins }

* [Synapse](plugins/synapse.md)
* [Внешний модуль аудита](plugins/gostech_audit_log.md)

### Обеспечение безопасности {: #security }

* [Работа в защищенной ОС](security/os.md)
* [Ограничение программной среды](security/runtime.md)
* [Журнал аудита в защищенной ОС](security/audit_log.md)
* [Контроль целостности](security/integrity.md)


<!-- План на развитие структуры документации:
### Ознакомление с ПО Picodata
* [Основные концепции](concepts)

### Инструкции и руководства
* Кластер в контейнерной среде
* Кластер с использованием Ansible
* Подключение и работа в веб-интерфейсе
* Управление пользователями и привилегиями
* Разработка плагинов
* Аварийное восстановление
* Резервное копирование
* Обновление Picodata

### Справочные материалы
* Справочник настроек

### Администрирование {: #admin }
* Использование журнала безопасности
* Перечень событий безопасности

### Архитектура
* Схема данных: таблицы, индексы
* Отказоустойчивость и репликация
* Масштабирование
* Алгоритм Raft
* Bootstrap
* Идентификация и аутентификация
* Управление доступом (авторизация) -->

На данном портале представлена техническая документация программного
продукта Picodata. Информация о выгодах сотрудничества с компанией
Picodata, корпоративных решениях и услугах, новостях, событиях находится
на сайте [picodata.io](https://picodata.io).

<a style="display: none" href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fdocs.picodata.io%2Fpicodata%2F&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false"/></a>
