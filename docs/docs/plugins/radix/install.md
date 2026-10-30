# Установка {: #install }

!!! note "Примечание"
    Особенности установки и настройки
    сертифицированных версий Radix приведены в отдельном
    [документе](certified.md)

## Развёртывание с помощью Ansible {: #radix_deploy_ansible }

При работе с Picodata в промышленной среде удобно использовать [роль
Picodata для Ansible]. С её помощью вы можете развернуть кластер СУБД
Picodata на нескольких узлах, в том числе, с поддержкой плагинов.

Для этого потребуется:

- [установить роль](../../admin/deploy_ansible.md#install_role)
- [подготовить плейбук](../../admin/deploy_ansible.md#create_playbook)
- [подготовить инвентарный
  файл](../../admin/deploy_ansible.md#create_inventory_file), включив в
  него блок параметров для установки Radix
- [установить кластер](../../admin/deploy_ansible.md#install_cluster) с Radix

### Конфигурация Radix для Ansible {: #radix_inventory_config }

Начиная с версии Picodata 26.1, плагин можно настроить, указав его
параметры для тиров (блок `tiers`), которые, в свою очередь, привязаны к
сервису плагина. Добавьте в инвентарный файл блок параметров,
относящийся к Radix:

```yaml
plugins:
  radix:                                                          # плагин
    path: '../files/radix_1.2.0-centos_el8.tar.gz'                # путь до пакета с Radix
    config: '../files/radix-config.yml'                           # путь до файла с настройками Radix
    services:
      radix:
        tiers:                                                    # список тиров, в которые устанавливается сервис radix
          - default:                                              # указано значение по умолчанию (default)
              listener:
                enabled: true
                listen: "0.0.0.0:73<INSTANCE_NUM>"
                advertise: "<INSTANCE_ADDR>:73<INSTANCE_NUM>"
                tls:
                  enabled: false

    migration_context:                                            # параметры миграции для Radix
      tier_for_db_0: 'default'
      tier_for_db_1: 'default'
      tier_for_db_2: 'default'
      tier_for_db_3: 'default'
      tier_for_db_4: 'default'
      tier_for_db_5: 'default'
      tier_for_db_6: 'default'
      tier_for_db_7: 'default'
      tier_for_db_8: 'default'
      tier_for_db_9: 'default'
      tier_for_db_10: 'default'
      tier_for_db_11: 'default'
      tier_for_db_12: 'default'
      tier_for_db_13: 'default'
      tier_for_db_14: 'default'
      tier_for_db_15: 'default'
      unlogged: ''
```

При настройке плагина можно использовать литералы, которые будут подставлены при разворачивании кластера:

- `<INSTANCE_ADDR>` — адрес сервера, на котором будет работать плагин
- `<INSTANCE_NUM>` — номер инстанса на сервере в двухзначном формате (01, 02, 03 ...)
- `<INSTANCE_NUM3>` — номер инстанса на сервере в трехзначном формате (001, 002, 003 ...)

### Пример инвентарного файла {: #inventory_example }

??? example "Пример инвентарного файла cluster.yml с указанием плагина Radix"
    ```yaml
    all:
      vars:
        ansible_user: vagrant      # пользователь для ssh-доступа к серверам

        repo: 'https://download.picodata.io'  # репозиторий, откуда инсталлировать пакет Picodata

        cluster_name: 'demo'           # имя кластера
        admin_password: '123asdZXV'    # пароль пользователя admin

        default_bucket_count: 16384    # количество бакетов в каждом тире (для Radix требуется значение 16384)

        audit: false                   # состояние аудита (отключён)
        log_level: 'info'              # уровень отладки
        log_to: 'file'                 # вывод журнала в файлы (а не в journald)

        conf_dir: '/etc/picodata'         # директория для хранения конфигурационных файлов
        data_dir: '/var/lib/picodata'     # директория для хранения данных
        run_dir: '/var/run/picodata'      # директория для хранения sock-файлов
        log_dir: '/var/log/picodata'      # директория для журналов и файлов аудита
        share_dir: '/usr/share/picodata'  # директория для размещения служебных данных (плагинов)

        listen_address: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
        pg_address: '{{ listen_address }}'       # адрес, по которому инстанс принимает подключения по PostgreSQL-протоколу

        first_bin_port: 13301     # начальный бинарный порт для первого инстанса
        first_http_port: 18001    # начальный http-порт для первого инстанса для веб-интерфейса
        first_pg_port: 15001      # начальный номер порта для PostgreSQL-протокола инстансов кластера

        tiers:                         # описание тиров
          arbiter:                     # имя тира
            replicaset_count: 1        # количество репликасетов
            replication_factor: 1      # фактор репликации
            config:
              memtx:
                memory: 64M            # количество памяти, выделяемое каждому инстансу тира
            host_groups:
              - ARBITERS               # целевая группа серверов для установки инстанса

          default:                     # имя тира
            replicaset_count: 3        # количество репликасетов
            replication_factor: 3      # фактор репликации
            bucket_count: 16384        # количество бакетов в тире
            config:
              memtx:
                memory: 71M            # количество памяти, выделяемое каждому инстансу тира
            host_groups:
              - STORAGES               # целевая группа серверов для установки инстанса

        db_config:                     # параметры конфигурации кластера (см. https://docs.picodata.io/picodata/stable/reference/db_config)
          governor_auto_offline_timeout: 30
          iproto_net_msg_max: 500
          memtx_checkpoint_count: 1
          memtx_checkpoint_interval: 7200

        plugins:
          radix:                                                        # плагин
            path: '../plugins/radix_1.2.0-1-ubuntu_noble.tar.gz'        # путь до пакета с Radix
            config: '../plugins/radix-config.yml'                       # путь до файла с настройками Radix
            services:
              radix:
                tiers:                                                  # список тиров, в которые устанавливается сервис radix
                  - default                                             # указано значение по умолчанию (default)
                listener:
                  enabled: true
                  listen: "0.0.0.0:73<INSTANCE_NUM>"
                  advertise: "<INSTANCE_ADDR>:73<INSTANCE_NUM>"
                  tls:
                    enabled: false
            migration_context:                                          # параметры миграции для Radix
              tier_for_db_0: 'default'
              tier_for_db_1: 'default'
              tier_for_db_2: 'default'
              tier_for_db_3: 'default'
              tier_for_db_4: 'default'
              tier_for_db_5: 'default'
              tier_for_db_6: 'default'
              tier_for_db_7: 'default'
              tier_for_db_8: 'default'
              tier_for_db_9: 'default'
              tier_for_db_10: 'default'
              tier_for_db_11: 'default'
              tier_for_db_12: 'default'
              tier_for_db_13: 'default'
              tier_for_db_14: 'default'
              tier_for_db_15: 'default'
              unlogged: ''
    DC1:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-1-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

        server-1-2:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.22' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'ARBITERS'        # определение целевой группы серверов для установки инстансов

    DC2:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-2-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.20.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

    DC3:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-3-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.21.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов
    ```

[роль Picodata для Ansible]: ../../admin/deploy_ansible.md#plugin_management

### Установка плагина в кластер {: #run_playbook }

Установите кластер Picodata с Radix, указав инвентарный файл и файл плейбука:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml
```

См. также:

- [Развёртывание кластера через Ansible](../../admin/deploy_ansible.md)

## Развёртывание с помощью Docker {: #radix_deploy_docker }

Доступны два образа:

- `<registry>/radix:1.2.0` — полноценный образ, готовый для
  [использования в Kubernetes](#radix_deploy_k8s). Не имеет преднастроек.
- `<registry>/radix:1.2.0-standalone` — образ с одиночным инстансом
  Picodata и предустановленным плагином Radix для быстрого ознакомления.
  Не предназначен для использования в производственной среде.

!!! note "Примечание"
    Доступ к репозиторию с образами Picodata и Radix предоставляется по запросу для клиентов Picodata.

Запуск одиночного образа:

```shell
docker pull <registry>/radix:1.2.0-standalone
docker run --rm -p 7379:7379 -p 4327:4327 -p 5327:5327 <registry>/radix:1.2.0-standalone
```

После старта плагин готов к работе:

```shell
redis-cli -p 7379 ping
```

Открытые порты:

| Порт | Протокол      |
| ---- | ------------- |
| 7379 | Redis (RESP2) |
| 4327 | PostgreSQL    |
| 5327 | HTTP          |

## Развёртывание с помощью Kubernetes {: #radix_deploy_k8s }

### Требования {: #radix_k8s_requirements }

Для данного способа требуются:

- Kubernetes и Minikube 1.30 или новее
- `kubectl` и `helm`
- `redis-cli` на рабочей машине (для проверки)
- Доступ из кластера Kubernetes к Docker-реестру от Picodata (предоставляется клиентам Radix по запросу)
- Сам Docker-образ, содержащий ненастроенные Picodata и Radix (кластер и плагин настраивает [оператор](#radix_k8s_install_operator))

Для начала запустите Minikube:

```shell
minikube start --driver=docker --cpus=4 --memory=8192
```

!!! note "Примечания"
    - В ресурсе `PicoclusterDB` нужно оставить `disableAutoAntiAffinity: true`, так как в кластере `minikube` один узел.
    - Временное событие `FailedScheduling` о несвязанных PVC не является ошибкой.

### Установка оператора {: #radix_k8s_install_operator }

Требуется оператор версии 0.14.2 или новее. Выполните следующие команды:

```shell
helm repo add picodata https://binary.picodata.io/repository/helm
helm repo update
helm install picodata-operator picodata/picodata-operator -n picodata-operator-system --create-namespace
kubectl rollout status deployment/picodata-operator-controller-manager -n picodata-operator-system --timeout=600s
```

### Пространство имён и секреты {: #radix_k8s_namespaces_and_secrets }

Кластер размещается в пространстве имён `picodata`. Пароль администратора Picodata хранится в Secret `picodata-admin-secret`, пустой пароль пользователя по умолчанию — в Secret `radix-default-user` (см. [Особенности авторизации](#radix_k8s_auth)):

```bash
kubectl create namespace picodata
kubectl create secret generic picodata-admin-secret --namespace picodata --from-literal=password=Radix123Pass
kubectl create secret generic radix-default-user --namespace picodata --from-literal=password=
```

Замените `Radix123Pass` на собственный пароль. Пароль должен удовлетворять парольной политике, настроенной в вашем кластере. По умолчанию требуются: одна заглавная латинская буква, одна строчная, одна цифра, не менее 8 символов.

### Настройка кластера {: #radix_k8s_cluster_config }

Добавьте описание кластера в конфигурационный файл `radix-cluster.yaml`:

```yaml
apiVersion: picodata.picodata.io/v1
kind: PicoclusterDB
metadata:
  name: radix
  namespace: picodata
spec:
  image:
    repository: <registry>
    tag: "radix:1.2.0"
    pullPolicy: IfNotPresent
  clusterName: radix
  adminPassword:
    secretName: picodata-admin-secret
    key: password
  cluster:
    defaultBucketCount: 16384
    shareDir: /usr/share/picodata
    plugins:
      - name: radix
        version: "1.2.0"
        users:
          - name: default
            passwordSecretRef: radix-default-user
            roles:
              - radix_reader
              - radix_writer
        migrationContext:
          tier_for_db_0: "default"
          tier_for_db_1: "default"
          tier_for_db_2: "default"
          tier_for_db_3: "default"
          tier_for_db_4: "default"
          tier_for_db_5: "default"
          tier_for_db_6: "default"
          tier_for_db_7: "default"
          tier_for_db_8: "default"
          tier_for_db_9: "default"
          tier_for_db_10: "default"
          tier_for_db_11: "default"
          tier_for_db_12: "default"
          tier_for_db_13: "default"
          tier_for_db_14: "default"
          tier_for_db_15: "default"
          unlogged: ""
  systemParameters:
    auth_password_length_min: "0"
    auth_password_enforce_uppercase: "false"
    auth_password_enforce_lowercase: "false"
    auth_password_enforce_digits: "false"
  tiers:
    - name: default
      replicasets: 1
      replicationFactor: 3
      canVote: true
      disableAutoAntiAffinity: true
      storage:
        size: 1Gi
      memtx:
        memory: "256M"
      resources:
        requests:
          cpu: "250m"
          memory: "512Mi"
      plugins:
        - name: radix
          services:
            - name: radix
              listenerPort: 7379
```

!!! note "Примечание"
    Все параметры конфигурации являются обязательными.

Особенности параметров конфигурации:

- `spec.image.repository` и `spec.image.tag` склеиваются через `/` в полное имя образа: `<registry>/radix:1.2.0`.
- `spec.clusterName` — имя кластера Picodata: значение попадает в параметр [`cluster.name`](../../reference/config.md#cluster_name) конфигурации Picodata. Имена подов и сервисов строятся из `metadata.name`.
- `spec.adminPassword` — ссылка на секрет с паролем администратора, созданным на предыдущем шаге.
- `spec.cluster.defaultBucketCount` — количество бакетов в тирах (параметр [default_bucket_count](../../reference/config.md#cluster_default_bucket_count)).
- `spec.cluster.shareDir` — директория, в которой образ хранит файлы плагинов.
- `spec.cluster.plugins[]` — плагин `radix` версии 1.2.0 и его [контекст миграций из 17 параметров](#plugin_enable_details).
- `spec.cluster.plugins[].users[]` — пользователь по умолчанию `default` с пустым паролем из секрета `radix-default-user` и ролями `radix_reader` и `radix_writer`. Оператор [создаёт пользователя](#radix_k8s_auth) и выдаёт роли после миграций плагина.
- `spec.systemParameters` — параметры, которые оператор применяет через `ALTER SYSTEM SET` при каждой синхронизации. Здесь они ослабляют парольную политику: без этого пустой пароль пользователя `default` не пройдёт валидацию.
- `spec.tiers[]` — один тир `default` с одним репликасетом и фактором репликации 3, то есть 3 инстанса: 1 мастер и 2 реплики. Поды получают имена `default-radix-1-0`, `default-radix-1-1` и `default-radix-1-2` (шаблон `<тир>-<metadata.name>-1-<N>`).
- `spec.tiers[].plugins[].services[].listenerPort` — порт, который слушает Redis (7379).
- `memtx.memory` — объём памяти под хранение данных инстанса, параметр [instance.memtx.memory](../../reference/config.md#instance_memtx_memory).
- `storage.size`, `memtx.memory` и `resources` — минимальные значения для примера (подбираются под нагрузку).
- `disableAutoAntiAffinity: true` — нужен только для кластеров Kubernetes из одного узла, например `minikube`.

!!! warning "Внимание"
    На тирах, где работает Radix, количество бакетов должно быть равно ровно
    16384. В данном примере тир один, поэтому достаточно
    `spec.cluster.defaultBucketCount: 16384`.

!!! warning "Внимание"
    При развёртывание в промышленной среде удалите строку
    `disableAutoAntiAffinity: true`. Тогда поды одного тира будут по
    возможности размещаться на разных узлах Kubernetes.

### Запуск кластера {: #radix_k8s_cluster_deploy }

Для запуска кластера выполните следующие команды:

```shell
kubectl apply -f radix-cluster.yaml
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=radix -n picodata --timeout=600s
kubectl wait --for=jsonpath='{.status.phase}'=Ready picoclusterdb/radix -n picodata --timeout=600s
kubectl get picoclusterdb radix -n picodata -o jsonpath='{.status.tiers[*].plugins}'
```

Ожидаемый статус плагина:

```json
[{"enabled":true,"name":"radix","version":"1.2.0"}]
```

Готовность подов не означает готовности плагина. Поды принимают
состояние `Ready` примерно за 30 секунд, после чего оператор выполняет
`CREATE PLUGIN`, `ADD SERVICE`, установку `migration_context`,
`MIGRATE`, создание пользователя по умолчанию и `ENABLE`. Процесс
занимает от нескольких секунд до пары минут и заканчивается переходом
ресурса `PicoclusterDB` из фазы `Initializing` в фазу `Ready` — её и
ждёт вторая команда `kubectl wait...`. Первая команда может завершиться
раньше, чем созданы все три пода; полноту кластера гарантирует именно
ожидание фазы `Ready`.

За прогрессом можно следить командой `kubectl get picoclusterdb radix -n
picodata -w` и по журналам оператора. Если фаза `Ready` не наступает,
см. [«Устранение неполадок»](#radix_k8s_troubleshooting).

### Адреса Radix {: #radix_k8s_addresses }

Конфигурацию листенера Radix оператор создаёт сам в ConfigMap `default-radix`, ручная настройка не требуется:

- `listen` — `0.0.0.0:7379`
- `advertise` — FQDN пода: `<под>.default-radix-interconnect.picodata.svc.cluster.local:7379`. В самом ConfigMap на этом месте хранится заглушка `__PLUGIN_ADVERTISE_RADIX_RADIX__`, которую init-контейнер пода подставляет при старте.

Клиенты внутри Kubernetes подключаются к этим адресам, их же возвращает команда `CLUSTER SLOTS`. Например, первый под доступен по адресу `default-radix-1-0.default-radix-interconnect.picodata.svc.cluster.local:7379`.

### Доступ внешних клиентов {: #radix_k8s_external_access }

Пример развёртывания предполагает клиентов внутри Kubernetes. Для доступа снаружи добавьте листенеру Radix поле `externalService` (в `spec.tiers[].plugins[].services[]`):

```yaml
...
          services:
            - name: radix
              listenerPort: 7379
              externalService:
                enabled: true
                perPod: true
...
```

Особенности параметров конфигурации:

- `enabled: true` — создаёт внешний Service типа LoadBalancer или NodePort
- `perPod: true` — создаёт отдельный LoadBalancer на каждый под: `default-radix-1-0-external` и так далее. Виртуальный IP-адрес (VIP) каждого пода становится его адресом `advertise`, и `CLUSTER SLOTS` возвращает клиентам VIP вместо внутрикластерных FQDN, поэтому клиентам в кластерном режиме нужен именно `perPod`. Для выдачи VIP требуется MetalLB или облачный балансировщик (этот вариант проверен в minikube с расширением `metallb`).

См. также:

- [Per-pod-сервисы для клиентов, работающих с шардированными данными](../../admin/deploy_kubernetes.md#external_service_perpod)
- [Внешний доступ](../../admin/deploy_kubernetes.md#external_access)

!!! warning "Внимание"
    При первом создании ресурса следует включить `externalService`.
    Включение `perPod` на работающем тире меняет адрес `advertise`, что
    не поддерживается в данный момент в Picodata.

### Особенности авторизации {: #radix_k8s_auth }

В Radix 1.2.0 `authorization_mode.state` по умолчанию равен `enabled`, а `authorization_mode.default_user_name` — `default`. Без этого пользователя листенер запускается, но каждый клиент получает ошибку `NOAUTH Authentication required`.

Пользователя создаёт оператор по блоку `users` ресурса
`radix-cluster.yaml`, отдельных действий не требуется. Для неявной
аутентификации пароль пользователя `default` должен быть пустым, поэтому
секрет `radix-default-user` содержит пустое значение. Пустой пароль
допустим только с методами аутентификации `md5` и `chap-sha1` (см.
[CREATE USER](../../reference/sql/create_user.md)) и запрещён парольной
политикой по умолчанию — её ослабляет блок `systemParameters`. После
создания кластера клиенты без явной команды `AUTH` аутентифицируются как
`default`: команда `ACL WHOAMI` возвращает `default`.

!!! warning "Внимание"
 Oслабленная парольная политика действует на весь кластер, и вернуть строгую политику при декларативном способе нельзя: оператор выполняет `ALTER USER "default"` с пустым паролем при каждой синхронизации, в то время как со строгой политикой синхронизации завершаются ошибкой `password is too short`, а фаза ресурса падает в `Initializing`. Если это недопустимо, используйте SQL-вариант [ниже](#radix_k8s_auth_sql) либо откажитесь от неявной аутентификации: задайте пользователю `default` полноценный пароль в Secret `radix-default-user`. Тогда блок `systemParameters` не нужен, а клиенты аутентифицируются явно.

### Настройка авторизации с помощью SQL {: #radix_k8s_auth_sql }

Если держать ослабленную парольную политику в ресурсе недопустимо, а пароль у пользователя по умолчанию должен быть пустым, то уберите блоки `users` и `systemParameters` из `radix-cluster.yaml` и создайте пользователя вручную после перехода `PicoclusterDB` в фазу `Ready` (роли `radix_reader` и `radix_writer` создаются миграциями плагина; до создания пользователя клиенты получают `NOAUTH`). Скрипт ослабляет парольную политику только на время своей работы. Сохраните его в файл `create-default-user.sql`:

```sql
ALTER SYSTEM SET auth_password_length_min TO 0;
ALTER SYSTEM SET auth_password_enforce_uppercase TO FALSE;
ALTER SYSTEM SET auth_password_enforce_lowercase TO FALSE;
ALTER SYSTEM SET auth_password_enforce_digits TO FALSE;
CREATE USER IF NOT EXISTS "default" WITH PASSWORD '';
GRANT radix_reader TO "default";
GRANT radix_writer TO "default";
ALTER SYSTEM SET auth_password_enforce_digits TO TRUE;
ALTER SYSTEM SET auth_password_enforce_lowercase TO TRUE;
ALTER SYSTEM SET auth_password_enforce_uppercase TO TRUE;
ALTER SYSTEM SET auth_password_length_min TO 8;
```

Примените команды из этого файла, подключившись к административной
консоли Picodata:

```shell
kubectl exec -i -n picodata default-radix-1-0 -c picodata -- picodata admin /pico/admin.sock < create-default-user.sql
```

### Проверка работы кластера {: #radix_k8s_check }

Проверьте доступность Radix на порту 7379:

```shell
kubectl port-forward -n picodata pod/default-radix-1-0 7379:7379 &
redis-cli -p 7379 PING          # PONG
redis-cli -p 7379 SET k1 v1     # OK
redis-cli -p 7379 GET k1        # v1
redis-cli -p 7379 CLUSTER SLOTS # 0..16383 + три FQDN подов
```

`kubectl port-forward` используется только для проверки; после неё процесс можно остановить. Рабочие клиенты подключаются изнутри Kubernetes к FQDN подов на порт 7379 (см. [«Адреса Radix»](#radix_k8s_addresses)).

### Определение мастера репликасета {: #radix_k8s_detect_replicaset_master }

Мастер репликасета определяется командой `INFO replication`: на мастере она возвращает `role:master` и `connected_slaves:2`. Также можно использовать `CLUSTER NODES`. Команда `ROLE` не поддерживается.

Подключаться можно к любому поду: запись, отправленная на реплику, проксируется на мастер без ответов `MOVED` или `READONLY`.

### Устранение неполадок {: #radix_k8s_troubleshooting }

- Клиенты получают `NOAUTH Authentication required` — не создан пользователь `default`: проверьте блок `users` в ресурсе и наличие Secret `radix-default-user`, см. [Особенности авторизации](#radix_k8s_auth).
- Статус плагина не `"enabled":true` или фаза `PicoclusterDB` надолго остаётся `Initializing` — смотрите журналы оператора (`kubectl logs deployment/picodata-operator-controller-manager -n picodata-operator-system`) и журналы подов кластера.
- Следующие сообщения в журналах не являются ошибками:
  - предупреждение `Plugin version 26.2.1 does not match picodata version 26.2.3`;
  - сообщение реплик `DdlCommit: awaiting DDL results from master replica` во время миграций.
- Пароль администратора доступен в переменной окружения `PICODATA_ADMIN_PASSWORD` пода любому, кто может выполнить в нём `kubectl exec`. Ограничьте право `exec` через RBAC.

### Удаление кластера {: #radix_k8s_cluster_remove }

```bash
kubectl delete picoclusterdb radix -n picodata
```

Команда удаляет StatefulSet, сервисы и ConfigMap кластера. PVC с данными
при этом сохраняются. Чтобы удалить также и данные:

```shell
kubectl delete pvc -n picodata --all
```

## Развёртывание вручную {: #radix_deploy_manual }

### Порядок действий {: #deploy_manual_steps }

Установка плагина Radix вручную имеет ряд особенностей. Процедура установки включает:

- создание файла конфигурации для инстанса Picodata, где в разделе
  `plugins` будут указаны параметры Radix ([пример](#picodata_config)).
- установку у [тиров][tier], на которые предполагается развернуть
  плагин, 16384 [бакетов]. См. описание [bucket_count] и
  [default_bucket_count], а также [пример](#picodata_config) конфигурации
- запуск инстанса Picodata с поддержкой плагинов (параметр [`--share-dir`])
- распаковку архива Radix в директорию, указанную на предыдущем шаге
- подключение к [административной консоли][admin_console] инстанса
- выполнение SQL-команд для:
    - регистрации плагина
    - привязки его сервиса к [тиру][tier]
    - миграции
    - создания пользователя, который будет использоваться в Radix по умолчанию
    - включения плагина в кластере

Данные шаги более подробно описаны ниже.

[`--share-dir`]: ../../reference/cli.md#run_share_dir
[admin_console]: ../../tutorial/connecting.md#admin_console
[tier]: ../../overview/glossary.md#tier
[бакетов]: ../../overview/glossary.md#bucket
[bucket_count]: ../../reference/config.md#cluster_tier_tier_bucket_count
[default_bucket_count]: ../../reference/config.md#cluster_default_bucket_count

См. также:

- [Установка плагинов](../../architecture/plugins.md#plugin_install)
- [Управление плагинами](../../dev/plugin_mgmt.md)

### Файл конфигурации Picodata для Radix {: #picodata_config }

Заполните файл конфигурации для инстанса Picodata, указав параметры
плагина Radix. Используйте следующий шаблон:

???+ example "cluster_config.yml: минимальная конфигурация кластера для запуска Radix"
      ```yml
      cluster:
        name: "demo_radix"
        tier:
          default:
            can_vote: true
            bucket_count: 16384
      instance:
        instance_dir: data
        memtx:
          memory: 2000000000
        share_dir: plugins-files
        pgproto:
          enabled: true
          listen: "0.0.0.0:7000"
        iproto:
          enabled: true
          listen: "0.0.0.0:8000"
        http:
          enabled: true
          listen: "0.0.0.0:9000"
        log:
          level: info
          format: plain
          destination: null
        plugin:
          radix:
            service:
              radix:
                listener:
                  enabled: true
                  listen: "0.0.0.0:7379"
                  advertise: "localhost:7379"
                  tls:
                    enabled: false
      ```

Запустите кластер с данным файлом конфигурации:

```shell
picodata run --config=cluster_config.yml
```

[Подключитесь](../../tutorial/connecting.md) к кластеру и перейдите к следующим шагам.

### Добавление плагина в кластере {: #plugin_add }

Radix поддерживает 16 баз данных, каждую из которых можно расположить на
отдельном тире. На одном тире можно разместить несколько баз данных.
Ниже будут примеры для одного и двух тиров.

Для регистрации плагина в кластере выполните следующую SQL-команду
в административной консоли Picodata:

```sql
CREATE PLUGIN radix 1.2.0;
```

Выполните указанные ниже шаги для того, чтобы включить плагин.

### Добавление сервиса и установка параметров {: #plugin_enable_details }

На данном этапе выполните следующие шаги:

- назначьте сервис плагина существующим тирам
- задайте значения для 16 параметров `migration_context.tier_for_db_N` (по числу баз данных в Radix)
- задайте значение для параметра `unlogged`

**Пример для одного тира (default)**

```sql
ALTER PLUGIN radix 1.2.0 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_0='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_1='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_2='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_3='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_4='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_5='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_6='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_7='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_8='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_9='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_10='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_11='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_12='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_13='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_14='default';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_15='default';
```

**Пример для двух тиров (hot/cold)**


```sql
ALTER PLUGIN radix 1.2.0 ADD SERVICE radix TO TIER hot;
ALTER PLUGIN radix 1.2.0 ADD SERVICE radix TO TIER cold;
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_0='hot';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_1='hot';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_2='hot';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_3='hot';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_4='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_5='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_6='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_7='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_8='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_9='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_10='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_11='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_12='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_13='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_14='cold';
ALTER PLUGIN radix 1.2.0 SET migration_context.tier_for_db_15='cold';
```

Параметр `unlogged` может принимать только два значения: `''` (пустая строка) для включённого WAL (режим Radix до версии 1.0.0):

```sql
ALTER PLUGIN radix 1.2.0 SET migration_context.unlogged='' OPTION(TIMEOUT=1200);
```

или `UNLOGGED` для отключённого:

```sql
ALTER PLUGIN radix 1.2.0 SET migration_context.unlogged='UNLOGGED' OPTION(TIMEOUT=1200);
```

!!! warning "Внимание!"
    При установке значения `UNLOGGED` данные на диск не пишутся, и
    сохраняются только в ОЗУ на [лидере репликасета]. Это означает,
    что перезапуск лидера приведёт к потере данных, которые на нём
    хранились. См. [подробнее](../../reference/sql/create_table.md#params) о
    режиме `UNLOGGED` при создании таблицы.

[лидере репликасета]: ../../overview/glossary.md#leader

### Запуск миграции {: #plugin_migrate }

Для выполнения миграции выполните команду:

```sql
ALTER PLUGIN radix MIGRATE TO 1.2.0 OPTION(TIMEOUT=300);
```

<!--
!!! note "Примечание"
    При обновлении кластера с более старой версии возможна ошибка:
    ```
    unknown migration files found in manifest migrations (mismatched hash checksum for migrations/0001_dbs.sql, was 0f720bee6e85b3b83e697b1554a79687, became 529c0e80bcb67ede4aa509448550724a)
    ```
    В этом случае следует отключить проверку контрольных сумм на время обновления:
    ```sql
    ALTER SYSTEM SET plugin_check_migration_hash = 'false';
    ```
    После этого провести миграцию и включить проверку обратно:
    ```sql
    ALTER SYSTEM SET plugin_check_migration_hash = 'true';
    ```
 -->

### Включение плагина {: #plugin_enable }

Перед включением плагина убедитесь, что в кластере создан пользователь,
указанный в параметре `default_user_name` секции
[authorization_mode](radix_settings.md#auth_mode) файла конфигурации плагина (по умолчанию это
`default`). При необходимости создайте этого пользователя в Picodata:

```sql
CREATE USER default WITH PASSWORD 'замените на ваш пароль' USING md5;
```

Данному пользователю также следует выдать роли `radix_reader` и
`radix_writer` (появляются в Picodata после выполнения
[миграций](#plugin_migrate) Radix) для работы с объектами БД:

```sql
GRANT radix_reader TO default;
GRANT radix_writer TO default;
```

После этого включите плагин:

```sql
ALTER PLUGIN radix 1.2.0 ENABLE OPTION(TIMEOUT=30);
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin;
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть
значение `true`.

Читать далее:

- [Настройка плагина](configuration.md)
