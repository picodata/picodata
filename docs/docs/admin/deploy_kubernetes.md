# Развёртывание через Kubernetes Operator

Kubernetes Operator — рекомендуемый способ управления кластером Picodata
в Kubernetes для production-окружений. Оператор реализует reconcile-loop
на основе собственного набора объектов (CRD, Custom Resource Definition)
`PicoclusterDB` и автоматически поддерживает желаемое состояние
кластера, создавая и обновляя контроллеры StatefulSet, сервисы
(Services), хранилища конфигураций (ConfigMaps), а также следит за
установкой и обновлением плагинов.

## Требования {: #requirements }

- Kubernetes 1.30+
- kubectl 1.28+

## Установка оператора {: #install }

Установка выполняется в два шага: сначала регистрируются CRD, затем
разворачивается сам оператор.

**Шаг 1.** Установите CRD:

```shell
kubectl apply -f https://git.picodata.io/core/picodata-operator/-/raw/stable/picodata-operator-deploy/crd.yaml
```

**Шаг 2.** Установите оператор:

```shell
kubectl apply -f https://git.picodata.io/core/picodata-operator/-/raw/stable/picodata-operator-deploy/operator.yaml
```

Дождитесь готовности пода оператора:

```shell
kubectl rollout status deployment/picodata-operator-controller-manager \
  -n picodata-operator-system
```

## Установка через Helm {: #helm }

Helm-чарты позволяют управлять оператором и кластерами декларативно, без ручного
применения манифестов.

### Установка оператора {: #helm_operator }

Установите оператор с помощью Helm:

```shell
helm install picodata-operator \
  https://git.picodata.io/core/picodata-operator/-/archive/stable/chart.tar.gz \
  -n picodata-operator-system --create-namespace
```

Дождитесь готовности:

```shell
kubectl rollout status deployment/picodata-operator-controller-manager \
  -n picodata-operator-system
```

!!! note "Обновление CRD"
    Helm добавляет CRD при первой установке, но **не обновляет** их при
    `helm upgrade`. При обновлении версии оператора применяйте CRD отдельно:

    ```shell
    kubectl apply -f https://git.picodata.io/core/picodata-operator/-/raw/vX.Y.Z/picodata-operator-deploy/crd.yaml
    ```

### Развёртывание кластера через Helm {: #helm_cluster }

Создайте файл значений `values.yaml` с конфигурацией кластера:

```yaml title="values.yaml"
image:
  tag: "26.1.5"

adminPassword:
  password: "<пароль>"

cluster:
  defaultReplicationFactor: 2
  defaultBucketCount: 3000

tiers:
  - name: default
    replicasets: 2
    replicationFactor: 2
    storage:
      size: 10Gi
    memtx:
      memory: "512M"
    pg:
      enabled: true
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
```

Установите кластер:

```shell
helm install my-cluster \
  https://git.picodata.io/core/picodata-operator/-/archive/stable/chart-cluster.tar.gz \
  -n picodata --create-namespace \
  -f values.yaml
```

Следите за готовностью:

```shell
kubectl get picoclusterdb -n picodata -w
```

### Обновление кластера {: #helm_upgrade }

Для обновления версии Picodata или изменения конфигурации:

```shell
helm upgrade my-cluster ./chart-cluster -n picodata --set image.tag=26.2.0
```

### Удаление {: #helm_delete }

`helm uninstall` **не удаляет** ресурс `PicoclusterDB` — данные защищены от
случайного удаления. Для полного удаления кластера:

```shell
kubectl delete picoclusterdb -n picodata my-cluster-picodata-cluster
helm uninstall my-cluster -n picodata
```

PVC (данные) остаются после удаления CR. Для полного удаления данных:

```shell
kubectl delete pvc -n picodata -l app.kubernetes.io/instance=my-cluster
```

## Создание namespace и секрета {: #secret }

Создайте namespace для кластера и секрет с паролем пользователя `admin`:

```shell
kubectl create namespace picodata

kubectl create secret generic picodata-admin-secret \
  --namespace picodata \
  --from-literal=password=<ваш-пароль>
```

!!! warning "Внимание!"
    Пароль должен соответствовать политике безопасности Picodata: не менее
    8 символов, латинские буквы в верхнем и нижнем регистрах, цифры.

## Описание ресурса PicoclusterDB {: #crd }

Кластер описывается ресурсом `PicoclusterDB`. Основные поля:

```yaml
apiVersion: picodata.picodata.io/v1
kind: PicoclusterDB
metadata:
  name: <имя-кластера>
  namespace: <namespace>
spec:
  image:
    repository: docker.binary.picodata.io  # реестр образов
    tag: "picodata:26.1.5"                 # образ и версия Picodata
    pullPolicy: IfNotPresent

  clusterName: <имя-кластера>              # имя кластера внутри Picodata

  adminPassword:
    secretName: picodata-admin-secret      # Secret с паролем admin
    key: password

  cluster:
    defaultReplicationFactor: 1  # фактор репликации по умолчанию для всех тиров
    defaultBucketCount: 3000     # количество бакетов (шардов)

  tiers:                         # список тиров кластера
    - name: <имя-тира>
      replicasets: 1             # количество репликасетов в тире
      replicationFactor: 1       # количество инстансов в каждом репликасете
      canVote: true              # участвует ли тир в Raft-голосовании
      storage:
        size: 1Gi
      memtx:
        memory: "128M"
      pg:
        enabled: true            # включить протокол PostgreSQL
      resources:
        requests:
          cpu: "100m"
          memory: "128Mi"
```

Общее количество подов тира равно `replicasets × replicationFactor`.<br>
Имена подов строятся по шаблону: `{тир}-{кластер}-{номер-репликасета}-{номер-инстанса}`.

## Развёртывание кластера {: #deploy }

### Минимальный кластер {: #deploy_simple }

Кластер из двух тиров: арбитр для Raft и основной тир для хранения данных.
Пример минимальной конфигурации:

```yaml title="picodata-cluster.yaml"
apiVersion: picodata.picodata.io/v1
kind: PicoclusterDB
metadata:
  name: picodata
  namespace: picodata
spec:
  image:
    repository: docker.binary.picodata.io
    tag: "picodata:26.1.5"
    pullPolicy: IfNotPresent

  clusterName: picodata

  adminPassword:
    secretName: picodata-admin-secret
    key: password

  cluster:
    defaultReplicationFactor: 1
    defaultBucketCount: 3000

  tiers:
    - name: arbiter
      replicasets: 1
      replicationFactor: 1
      canVote: true
      storage:
        size: 1Gi
      memtx:
        memory: "64M"
      pg:
        enabled: false
      resources:
        requests:
          cpu: "100m"
          memory: "128Mi"

    - name: default
      replicasets: 1
      replicationFactor: 2
      canVote: false
      storage:
        size: 10Gi
      memtx:
        memory: "512M"
      pg:
        enabled: true
      resources:
        requests:
          cpu: "500m"
          memory: "512Mi"
```

Примените манифест:

```shell
kubectl apply -f picodata-cluster.yaml
```

### Наблюдение за запуском {: #deploy_watch }

Следите за готовностью подов:

```shell
kubectl get pods -n picodata -w
```

Ожидаемый результат (все поды `1/1 Running`):

```
NAME                  READY   STATUS    RESTARTS   AGE
arbiter-picodata-1-0  1/1     Running   0          3m
default-picodata-1-0  1/1     Running   0          3m
default-picodata-1-1  1/1     Running   0          2m
```

Проверьте состояние CR:

```shell
kubectl get picoclusterdb -n picodata
```

```
NAME       READY   AGE
picodata   true    5m
```

### Фазы кластера {: #phases }

Поле `.status.phase` отражает текущее состояние кластера:

| Фаза           | Описание                                                                       |
|----------------|--------------------------------------------------------------------------------|
| `Pending`      | пользовательский ресурс (CR) создан, оператор начинает первичную инициализацию |
| `Initializing` | Ресурсы (StatefulSet, Service) создаются, кластер формируется                  |
| `Ready`        | Все поды запущены, кластер готов к работе                                      |
| `Degraded`     | Часть подов недоступна, кластер работает в деградированном режиме              |
| `Unknown`      | Состояние кластера не удалось определить                                       |
| `Deleting`     | CR помечен на удаление, идёт очистка ресурсов                                  |

Наблюдайте за сменой фаз в реальном времени:

```shell
kubectl get picoclusterdb -n picodata -w
```

При удалении (`kubectl delete picoclusterdb`) оператор переводит кластер в фазу
`Deleting` и удерживает CR через финализатор до завершения очистки. После этого CR
исчезает из API.

## Подключение к кластеру {: #connect }

### Протокол PostgreSQL (psql) {: #connect_psql }

Пробросьте pgproto-порт одного из подов тира `default`:

```shell
kubectl port-forward -n picodata pod/default-picodata-1-0 5432:5432
```

Подключитесь через psql:

```shell
psql "host=localhost port=5432 user=admin password=<пароль> dbname=picodata sslmode=disable"
```

Проверьте состояние инстансов через системную таблицу [`_pico_instance`]:

```sql
SELECT name, replicaset_name, current_state, tier
FROM _pico_instance;
```

[`_pico_instance`]: ../architecture/system_tables.md#_pico_instance

### Веб-интерфейс (Web UI) {: #connect_webui }

Пробросьте HTTP-порт:

```shell
kubectl port-forward -n picodata svc/default-picodata 8081:8081
```

Откройте [http://localhost:8081](http://localhost:8081) для просмотра состояния
кластера в веб-интерфейсе.

## Внешний доступ {: #external_access }

### Ingress {: #ingress }

Ingress обеспечивает доступ к веб-интерфейсу и метрикам (HTTP, порт 8081)
через доменное имя. Требует установленного Ingress-контроллера (например, nginx).

Включите Ingress в манифесте тира:

```yaml
tiers:
  - name: default
    ingress:
      enabled: true
      host: picodata.example.com
      ingressClassName: nginx
      annotations:
        nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
      tls:
        - secretName: picodata-tls
          hosts:
            - picodata.example.com
```

После применения манифеста веб-интерфейс будет доступен по адресу
`https://picodata.example.com`.

!!! note "Примечание"
    Ingress даёт доступ только к HTTP-интерфейсу (порт 8081).
    Для доступа по протоколу PostgreSQL (порт 5432) используйте `externalService`.

### LoadBalancer / NodePort {: #external_service }

Для доступа к [PostgreSQL-интерфейсу] снаружи кластера используйте
поле `externalService`:

```yaml
tiers:
  - name: default
    externalService:
      enabled: true
      type: LoadBalancer   # или NodePort
```

[PostgreSQL-интерфейсу]: ../tutorial/connecting.md#postgresql

При `type: LoadBalancer` Kubernetes создаёт отдельный объект Service с внешним IP.
При `type: NodePort` PostgreSQL-интерфейс будет доступен на фиксированном порту каждого узла.

Получите внешний адрес:

```shell
kubectl get svc -n picodata -l picodata.io/service-type=external
```

### Per-pod-сервисы для клиентов, работающих с шардированными данными {: #external_service_perpod }

Драйверы [picodata-jdbc] и [picopyn] учитывают распределение данных по
шардам и умеют маршрутизировать запросы напрямую на под, хранящий нужный
шард, без излишней нагрузки на сеть. Чтобы это работало, каждый под
должен объявить **свой собственный** внешний адрес в
`_pico_peer_address`.

[picodata-jdbc]: ../dev/connectors/jdbc.md
[picopyn]: ../dev/connectors/picopyn.md

Включите `perPod: true` в секции `externalService`:

```yaml
tiers:
  - name: default
    externalService:
      enabled: true
      type: LoadBalancer
      perPod: true
```

Оператор создаст один `LoadBalancer` Service на каждый под тира. MetalLB
(или cloud LB) назначит каждому Service уникальный VIP. Init-контейнер дождётся
назначения VIP и передаст его Picodata в качестве advertise-адреса — Picodata
объявит адрес вида `{vip}:{port}` в таблице `_pico_peer_address`.

!!! note "Требования"
    Необходим MetalLB (или облачный провайдер с поддержкой LoadBalancer) с пулом
    IP-адресов достаточного размера: минимум N + 1 адресов на тир (N подов +
    один tier-level Service).

Проверьте назначенные VIP и созданные Services:

```shell
kubectl get svc -n picodata -l picodata.io/external-pod-service=true
```

Проверьте адреса в таблице `_pico_peer_address`:

```sql
SELECT raft_id, address, connection_type
FROM _pico_peer_address
WHERE connection_type = 'pgproto';
```

```
 raft_id |        address         | connection_type
---------+------------------------+-----------------
       1 | 192.168.1.201:5432     | pgproto
       2 | 192.168.1.202:5432     | pgproto
```

### Настройка MetalLB для per-pod VIP {: #metallb_setup }

#### L2-режим (простой, без BGP) {: #metallb_l2 }

L2-режим работает без дополнительного сетевого оборудования: MetalLB
отвечает на ARP-запросы за выбранным IP-адресом. Подходит для разработки
и развёртывания небольших кластеров. Ограничение: весь трафик на VIP
проходит через один узел («спикер»).

```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: picodata-pool
  namespace: metallb-system
spec:
  addresses:
    - 192.168.1.200-192.168.1.229   # пул из ≥ N+1 адресов на тир
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: picodata-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - picodata-pool
```

#### BGP-режим (рекомендуется для production) {: #metallb_bgp }

В BGP-режиме каждый узел анонсирует маршруты к VIP напрямую в сеть через
протокол BGP. Преимущества перед L2:

- трафик распределяется по всем узлам через ECMP;
- failover быстрее (секунды по BGP keepalive vs десятки секунд ARP);
- не нужен ARP-прокси, работает в любой IP-сети.

**Предварительные требования:**

- маршрутизатор (или ToR-коммутатор) с поддержкой BGP;
- договорённые ASN: `localASN` (ASN кластера Kubernetes), `peerASN` (ASN роутера);
- IP-адрес BGP-пира со стороны роутера.

**Конфигурация MetalLB:**

```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: picodata-pool
  namespace: metallb-system
spec:
  addresses:
    - 192.168.1.200-192.168.1.229   # пул из ≥ N+1 адресов на тир
---
apiVersion: metallb.io/v1beta2
kind: BGPPeer
metadata:
  name: upstream-router
  namespace: metallb-system
spec:
  myASN: 64512          # ASN кластера Kubernetes (private range: 64512–65534)
  peerASN: 64513        # ASN upstream-роутера
  peerAddress: 192.168.0.1   # IP BGP-пира (роутер / ToR-коммутатор)
---
apiVersion: metallb.io/v1beta1
kind: BGPAdvertisement
metadata:
  name: picodata-bgp
  namespace: metallb-system
spec:
  ipAddressPools:
    - picodata-pool
  peers:
    - upstream-router
```

После применения конфигурации MetalLB установит BGP-сессию с роутером и начнёт
анонсировать маршруты к VIP из пула `picodata-pool`. Проверить сессию:

```shell
kubectl logs -n metallb-system -l component=speaker | grep -i bgp | tail -20
```

BGP-сессия должна перейти в состояние `Established`.

!!! tip "Несколько BGP-пиров"
    Если в кластере несколько ToR-коммутаторов, добавьте несколько ресурсов
    `BGPPeer` — MetalLB будет анонсировать маршруты каждому из них.

#### BGP через Cilium (альтернатива MetalLB) {: #cilium_bgp }

Если кластер использует Cilium в качестве CNI, MetalLB не нужен: Cilium
включает собственный **BGP Control Plane** и умеет назначать LoadBalancer IP
из внутреннего пула, анонсируя маршруты через BGP.

**Предварительные требования:**

- Cilium ≥ 1.13 с включённым `bgpControlPlane.enabled=true`
- маршрутизатор с поддержкой BGP

**Пул IP-адресов** (`CiliumLoadBalancerIPPool`):

```yaml
apiVersion: "cilium.io/v2alpha1"
kind: CiliumLoadBalancerIPPool
metadata:
  name: picodata-pool
spec:
  cidrs:
    - cidr: "192.168.1.200/27"   # 30 адресов — достаточно для N подов + tier LB
```

**BGP-политика** (`CiliumBGPPeeringPolicy`):

```yaml
apiVersion: "cilium.io/v2alpha1"
kind: CiliumBGPPeeringPolicy
metadata:
  name: picodata-bgp
spec:
  nodeSelector:
    matchLabels:
      kubernetes.io/os: linux   # применяется ко всем узлам
  virtualRouters:
    - localASN: 64512
      exportPodCIDR: false
      neighbors:
        - peerAddress: "192.168.0.1/32"   # IP upstream-роутера
          peerASN: 64513
      serviceSelector:
        matchExpressions:
          - key: picodata.io/external-pod-service
            operator: In
            values: ["true"]
          - key: picodata.io/cluster
            operator: Exists
```

`serviceSelector` ограничивает анонс только per-pod сервисами Picodata.
Это предотвращает утечку внутренних ClusterIP в BGP-таблицу роутера.

Проверить что Cilium установил BGP-сессию:

```shell
cilium bgp peers
```

```
Node            Local AS   Peer AS   Peer Address    Session State   Since   Received  Sent
k8s-node-01     64512      64513     192.168.0.1     Established     1h      12        15
k8s-node-02     64512      64513     192.168.0.1     Established     1h      12        15
```

!!! note "Cilium 1.16+"
    В Cilium 1.16 появился новый BGP API (`CiliumBGPClusterConfig`, `CiliumBGPPeerConfig`,
    `CiliumBGPAdvertisement`). Показанный выше `CiliumBGPPeeringPolicy` поддерживается
    начиная с Cilium 1.13 и работает во всех версиях. Для новых установок
    смотрите [документацию Cilium](https://docs.cilium.io/en/stable/network/bgp-control-plane/).

### Shard-aware без LoadBalancer: DNS + BGP pod routing {: #shard_aware_dns }

Если CNI кластера анонсирует pod CIDR в сеть через BGP (Cilium или Calico в BGP-режиме),
LoadBalancer-сервисы для shard-aware доступа **не нужны**. Pod IP становятся
маршрутизируемыми напрямую — без MetalLB и без per-pod VIP.

В этом случае оператор работает в режиме по умолчанию: advertise-адрес каждого
пода в `_pico_peer_address` — это стабильный FQDN:

```
hot-mycluster-1-0.hot-mycluster-interconnect.picodata.svc.cluster.local:5432
```

Shard-aware драйвер читает эти FQDN из `_pico_peer_address`, резолвит их в pod IP
и подключается напрямую.

**Что нужно:**

1. **Pod IP маршрутизируемы снаружи** — CNI в BGP-режиме анонсирует pod CIDR
   в сеть, клиент может достучаться до `10.244.x.x` напрямую.

2. **Kubernetes DNS доступен с клиента** — один из вариантов:
    - DNS forwarding: настроить на клиентской машине пересылку зоны
      `cluster.local` на IP сервиса `kube-dns` (если он доступен из сети);
    - Делегирование зоны: сделать `cluster.local` поддоменом вашего DNS,
      делегировав его на CoreDNS кластера.

**Что не нужно настраивать в `PicoclusterDB`:**

```yaml
tiers:
  - name: hot
    externalService:
      enabled: true
      type: LoadBalancer   # tier-level LB для bootstrap (опционально)
      # perPod: НЕ указывать — он не нужен
```

`perPod: true` и связанные per-pod Services не создаются. Оператор не выделяет
IP из пула MetalLB на каждый под.

**Схема подключения:**

```
Клиент (вне кластера)
  │
  ├─ DNS: hot-mycluster-1-0.hot-mycluster-interconnect.picodata.svc.cluster.local
  │    └─ forwarding *.cluster.local → CoreDNS кластера
  │    └─ ответ: 10.244.1.5  ← pod IP
  │
  └─ TCP 10.244.1.5:5432
       └─ маршрут через BGP: CNI анонсирует pod CIDR наружу
```

!!! note "Ограничение"
    Подход работает только если клиент находится в той же L3-сети, куда CNI
    анонсирует pod CIDR. Для клиентов за NAT или через интернет по-прежнему
    нужны LoadBalancer-сервисы с публичными IP.

## Управление плагинами {: #plugins }

### Сборка образа с плагином {: #plugins_image }

Стандартный образ Picodata не включает плагины. Для запуска плагина необходимо
собрать кастомный Docker-образ, распаковав архив плагина в директорию
`shareDir` (по умолчанию `/usr/share/picodata`).

Пример `Dockerfile` для плагина Radix:

```dockerfile
FROM docker.binary.picodata.io/picodata:26.1.5 as release

USER root

RUN mkdir -p /usr/share/picodata

COPY radix_1.0.1-centos_el8.tar.gz /usr/share/picodata/radix.tar.gz

RUN cd /usr/share/picodata && tar -xvzf radix.tar.gz && rm radix.tar.gz

RUN chown -R picodata:picodata /usr/share/picodata

USER picodata
```

После сборки и публикации образа укажите его в манифесте кластера:

```yaml
spec:
  image:
    repository: registry.example.com/picodata-with-radix
    tag: "26.1.2-radix-1.0.1"
```

### Описание плагина в манифесте {: #plugins_spec }

Плагины объявляются на двух уровнях: кластера и тира.

На **кластерном уровне** задаётся версия плагина и параметры первичной миграции:

```yaml
spec:
  cluster:
    shareDir: /usr/share/picodata  # путь к директории с .so-файлами плагинов
    plugins:
      - name: radix
        version: "1.0.0"
        migrationContext:
          tier_for_db_0: "default"
          # ... остальные параметры
```

На **уровне тира** задаётся привязка сервисов плагина к тиру и listener-порт:

```yaml
spec:
  tiers:
    - name: default
      plugins:
        - name: radix
          services:
            - name: radix
              listenerPort: 8082
```

После применения манифеста оператор автоматически выполнит полный жизненный цикл
установки: `CREATE PLUGIN`&nbsp;→ `ADD SERVICE TO TIER`&nbsp;→ `SET migration_context`&nbsp;→
`MIGRATE TO`&nbsp;→ `ENABLE`. Установка запускается только когда все поды тира готовы.

Статус установки плагина:

```shell
kubectl get picoclusterdb picodata -n picodata \
  -o jsonpath='{.status.tiers[*].plugins}'
```

Подробнее: [Управление плагинами](../dev/plugin_mgmt.md)

## Обновление версии Picodata {: #update }

Для обновления версии Picodata измените поле `spec.image.tag` в манифесте
и примените его:

```yaml
spec:
  image:
    tag: "picodata:26.2.0"
```

```shell
kubectl apply -f picodata-cluster.yaml
```

Оператор выполнит безопасное обновление:

1. Отключит (`DISABLE`) все включённые плагины, несовместимые с новым образом
2. Дождётся фиксации изменения в Raft-кластере
3. Обновит образ во всех StatefulSet-ах (rolling update)
4. Установит и включит плагины для новой версии

!!! note "Примечание"
    PVC (данные) при обновлении образа не затрагиваются.

## Ребутстрап инстанса {: #rebootstrap }

Ребутстрап применяется когда инстанс не может вернуться в кластер после
потери данных или повреждения тома. Оператор исключает инстанс из кластера
(`expel`), стирает его данные и позволяет ему заново присоединиться как
новому члену репликасета.

!!! warning "Внимание!"
    Ребутстрап возможен только если репликасет содержит более одного инстанса
    (replicationFactor ≥ 2) и хотя бы один из оставшихся инстансов доступен.

Создайте ресурс `PicoclusterInstanceRebootstrap`:

```yaml title="rebootstrap.yaml"
apiVersion: picodata.picodata.io/v1
kind: PicoclusterInstanceRebootstrap
metadata:
  name: rebootstrap-1
  namespace: picodata
spec:
  clusterRef:
    name: picodata           # имя PicoclusterDB
  instanceName: default-picodata-1-0  # имя пода (инстанса)
```

```shell
kubectl apply -f rebootstrap.yaml
```

Следите за фазами выполнения:

```shell
kubectl get picoclusterinstancerebootstrap -n picodata -w
```

```
NAME            INSTANCE               PHASE       AGE
rebootstrap-1   default-picodata-1-0   Expelling   10s
rebootstrap-1   default-picodata-1-0   Wiping      30s
rebootstrap-1   default-picodata-1-0   Rejoining   45s
rebootstrap-1   default-picodata-1-0   Succeeded   2m
```

После завершения ресурс `PicoclusterInstanceRebootstrap` можно удалить.

## Резервное копирование {: #backup }

!!! warning "Экспериментальная функция"
    Резервное копирование и восстановление находятся в стадии эксперимента.
    Не используйте их в production без предварительного тестирования.

### Настройка хранилища резервных копий {: #backup_storage }

Добавьте в манифест кластера секцию `backup` с описанием тома для хранения
резервных копий:

```yaml
spec:
  backup:
    mountPath: /pico/backup   # путь внутри пода, куда монтируется том
    volume:
      # Для production используйте NFS или другой тип тома с ReadWriteMany.
      # hostPath подходит только для одноузловых кластеров (minikube).
      nfs:
        server: nfs.example.com
        path: /exports/picodata-backup
```

### Ограничения резервного копирования {: #backup_limitations }

- Резервная копия запускается только если **все поды кластера готовы** (`1/1 Running`).
  При недоступности хотя бы одного пода бэкап перейдёт в фазу `Failed`
- Для одного кластера **одновременно может выполняться только один** `PicoclusterBackup`.
- Хранилище резервных копий должно быть **доступно на запись** со всех узлов кластера.
  Для multi-node-окружений используйте NFS или другой том с поддержкой ReadWriteMany.
  Оператор проверяет доступность тома перед запуском бэкапа (preflight-проверка)
- При **перезапуске пода** в ходе бэкапа консистентность резервной копии не гарантируется.
  Факт перезапуска фиксируется в `status.podSnapshots` для последующей диагностики
- По истечении `spec.backupTimeout` оператор прерывает операцию через
  `pico.abort_ddl()`. Прерванный бэкап **не пригоден для восстановления**

### Создание резервной копии {: #backup_create }

Создайте ресурс `PicoclusterBackup`:

```yaml title="backup.yaml"
apiVersion: picodata.picodata.io/v1
kind: PicoclusterBackup
metadata:
  name: backup-1
  namespace: picodata
spec:
  clusterRef:
    name: picodata           # имя PicoclusterDB
  backupTimeout: 3600        # максимальное время ожидания (секунды)
```

```shell
kubectl apply -f backup.yaml
```

Следите за статусом:

```shell
kubectl get picoclusterbackup -n picodata -w
```

```
NAME       CLUSTER    STATUS      AGE
backup-1   picodata   Succeeded   5m
```

Путь к сохранённой резервной копии:

```shell
kubectl get picoclusterbackup backup-1 -n picodata \
  -o jsonpath='{.status.backupDir}'
```

### Ограничения восстановления {: #restore_limitations }

- Восстановление — **деструктивная операция**: оператор останавливает весь кластер
  (scale до 0), стирает данные каждого инстанса и заменяет их содержимым снапшота
- Для одного кластера **одновременно может выполняться только одна** операция
  восстановления
- При `backupRef` ссылаемый `PicoclusterBackup` должен быть в фазе `Succeeded`.
- Если восстановление завершилось с ошибкой, **кластер остаётся остановленным**
  (все StatefulSet масштабированы до 0) для ручного разбора причин.
  Автоматического отката нет.
- Хранилище резервных копий должно быть **смонтировано в те же поды**, что и при
  создании бэкапа: оператор ожидает структуру `<mountPath>/<instanceName>/<backupDir>/`

### Восстановление из резервной копии {: #restore }

Создайте ресурс `PicoclusterRestore`:

```yaml title="restore.yaml"
apiVersion: picodata.picodata.io/v1
kind: PicoclusterRestore
metadata:
  name: restore-1
  namespace: picodata
spec:
  clusterRef:
    name: picodata           # имя PicoclusterDB для восстановления
  backupRef:
    name: backup-1           # ссылка на ресурс PicoclusterBackup
```

Если ресурс `PicoclusterBackup` был удалён, укажите директорию напрямую:

```yaml
spec:
  clusterRef:
    name: picodata
  backupDir: "20260508T143720"  # имя поддиректории в томе резервных копий
```

```shell
kubectl apply -f restore.yaml
```

## Anti-affinity {: #anti_affinity }

По умолчанию оператор автоматически добавляет правило `podAntiAffinity`,
которое запрещает размещение двух подов одного репликасета на одном узле
Kubernetes. Это гарантирует отказоустойчивость при потере узла.

### Требование к StorageClass {: #anti_affinity_storage }

Anti-affinity работает корректно только если StorageClass использует
`volumeBindingMode: WaitForFirstConsumer`.

При режиме `Immediate` (значение по умолчанию во многих дистрибутивах)
PVC создаются и привязываются к узлам **до** планирования подов. В результате
оба PVC одного репликасета могут оказаться на одном узле, и поды не смогут
«разойтись» — один из них зависнет в состоянии `Pending`.

Проверьте режим вашего StorageClass:

```sh
kubectl get storageclass -o custom-columns=NAME:.metadata.name,BINDING:.volumeBindingMode
```

Если значение `Immediate`, создайте StorageClass с нужным режимом:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: picodata-sc
provisioner: <ваш-provisioner>   # например, rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Retain
```

И укажите его в тире:

```yaml
tiers:
  - name: default
    storage:
      size: 10Gi
      storageClassName: picodata-sc
```

!!! note "Примечание"
    В управляемых кластерах (GKE, EKS, AKS) стандартные StorageClass обычно
    уже используют `WaitForFirstConsumer`. Проблема наиболее характерна для
    self-hosted-кластеров с local-path, Longhorn или OpenEBS.

### Отключение anti-affinity {: #anti_affinity_disable }

Для развёртывания на одноузловом кластере (minikube, dev-окружение) отключите
anti-affinity для нужного тира:

```yaml
tiers:
  - name: default
    disableAutoAntiAffinity: true
```

!!! warning "Внимание!"
    Не отключайте anti-affinity в production: при потере узла могут упасть
    сразу несколько инстансов одного репликасета.

## PodDisruptionBudget {: #pdb }

Оператор автоматически создаёт `PodDisruptionBudget` для тиров с фактором
репликации ≥ 3. PDB ограничивает количество одновременно недоступных подов
при плановых операциях (drain-узлы, обновление кластера Kubernetes), сохраняя
кворум кластера.

При необходимости выполнить drain на кластере без свободных узлов — когда
Kubernetes не может переместить поды — временно отключите PDB:

```yaml
tiers:
  - name: default
    disablePDB: true
```

После завершения drain верните значение в `false` (или удалите поле).

!!! warning "Внимание!"
    Не оставляйте PDB отключённым в production. Без PDB плановое обслуживание
    узлов может нарушить кворум кластера.

## Удаление кластера {: #delete }

```shell
kubectl delete picoclusterdb picodata -n picodata
```

Удаление пользовательского ресурса (CR) автоматически удаляет все
связанные с ним ресурсы (StatefulSet, Service, ConfigMap) через механизм
ownerReference.

!!! warning "Внимание!"
    PVC (постоянные тома с данными) при удалении CR **не удаляются** — это
    защита от случайной потери данных. Для полного удаления данных выполните:
    ```shell
    kubectl delete pvc -n picodata --all
    ```

## Мониторинг {: #monitoring }

Каждый инстанс Picodata отдаёт метрики в формате Prometheus на порту `8081`,
путь `/metrics`.

### ServiceMonitor через CR {: #monitoring_servicemonitor }

Если в кластере установлен [Prometheus Operator], включите автоматический
сбор метрик в манифесте кластера:

```yaml
spec:
  serviceMonitor:
    enabled: true
    interval: "30s"
    labels:
      release: kube-prometheus-stack   # должен совпадать с serviceMonitorSelector вашего Prometheus
```

[Prometheus Operator]: https://prometheus-operator.dev/

Оператор создаёт по одному `ServiceMonitor` на каждый тир — Prometheus
начнёт получать данные от каждого инстанса через headless-сервис.

### PodMonitor с job-лейблом по имени кластера {: #monitoring_podmonitor }

Для использования с Grafana-дашбордом рекомендуется настроить `PodMonitor`
так, чтобы label `job` в Prometheus соответствовал имени кластера. Это
позволяет выбирать кластер в выпадающем меню дашборда:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: picodata
  namespace: picodata
  labels:
    release: kube-prometheus-stack
spec:
  jobLabel: app.kubernetes.io/instance   # job = clusterName
  selector:
    matchExpressions:
      - key: app.kubernetes.io/instance
        operator: Exists
  namespaceSelector:
    matchNames:
      - picodata
  podMetricsEndpoints:
    - port: http
      path: /metrics
      interval: 30s
```

Один ресурс охватывает все кластеры в пространстве имён (namespace) и не требует изменений
при добавлении новых кластеров.

### Grafana-дашборд {: #monitoring_grafana }

Официальный дашборд Picodata доступен в репозитории `core/picodata`:
[monitoring/dashboard/Picodata.json](https://github.com/picodata/picodata/blob/master/monitoring/dashboard/Picodata.json)

Импортировать через UI Grafana: **Dashboards → Import → Upload JSON file**
или через URL файла.

После импорта выберите datasource Prometheus и нужный кластер в выпадающем меню `job`.

Читайте далее:

- [Dashboard для Grafana](../admin/grafana_monitoring.md)
- [Управление плагинами](../dev/plugin_mgmt.md)
