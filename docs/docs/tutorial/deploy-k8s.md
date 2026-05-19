# Развёртывание кластера через Kubernetes Operator {: #operator }

Kubernetes Operator — рекомендуемый способ управления кластером Picodata в
Kubernetes для production-окружений. Оператор реализует reconcile-loop на
основе CRD `PicoclusterDB` и автоматически поддерживает желаемое состояние
кластера: создаёт и обновляет StatefulSet-ы, Service-ы, ConfigMap-ы, следит
за установкой и обновлением плагинов.

## Требования {: #requirements }

- Kubernetes 1.30+
- kubectl 1.28+

## Установка оператора {: #install }

Установка выполняется в два шага: сначала регистрируются CRD, затем
разворачивается сам оператор.

**Шаг 1.** Установите CRD:

```shell
kubectl apply -f https://git.picodata.io/core/picodata-operator/-/raw/master/picodata-operator-deploy/crd.yaml
```

**Шаг 2.** Установите оператор:

```shell
kubectl apply -f https://git.picodata.io/core/picodata-operator/-/raw/master/picodata-operator-deploy/operator.yaml
```

Дождитесь готовности пода оператора:

```shell
kubectl rollout status deployment/picodata-operator-controller-manager \
  -n picodata-operator-system
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
    tag: "picodata:26.1.2"                 # образ и версия Picodata
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
      replicas: 1                # количество репликасетов в тире
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

Общее количество подов тира равно `replicas × replicationFactor`.<br>
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
    tag: "picodata:26.1.2"
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
      replicas: 1
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
      replicas: 1
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

Для доступа к PostgreSQL-интерфейсу (pgproto) снаружи кластера используйте
поле `externalService`:

```yaml
tiers:
  - name: default
    externalService:
      enabled: true
      type: LoadBalancer   # или NodePort
```

При `type: LoadBalancer` Kubernetes создаёт отдельный Service с внешним IP.
При `type: NodePort` pgproto будет доступен на фиксированном порту каждой ноды.

Получите внешний адрес:

```shell
kubectl get svc -n picodata -l picodata.io/service-type=external
```

## Управление плагинами {: #plugins }

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
- Хранилище резервных копий должно быть **доступно на запись** со всех нод кластера.
  Для multi-node окружений используйте NFS или другой том с поддержкой ReadWriteMany.
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
при плановых операциях (drain ноды, обновление кластера Kubernetes), сохраняя
кворум кластера.

При необходимости выполнить drain на кластере без свободных нод — когда
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

Удаление CR автоматически удаляет все связанные ресурсы (StatefulSet, Service,
ConfigMap) через механизм ownerReference.

!!! warning "Внимание!"
    PVC (постоянные тома с данными) при удалении CR **не удаляются** — это
    защита от случайной потери данных. Для полного удаления данных выполните:

    ```shell
    kubectl delete pvc -n picodata --all
    ```

## Мониторинг {: #monitoring }

Оператор поддерживает сбор метрик через [Prometheus Operator]. Включите
`ServiceMonitor` в манифесте кластера:

```yaml
spec:
  serviceMonitor:
    enabled: true
    interval: "30s"
```

[Prometheus Operator]: https://prometheus-operator.dev/

При наличии Prometheus Operator в кластере метрики каждого инстанса Picodata
будут автоматически обнаружены и собираться с эндпоинта `/metrics`.

Читайте далее:

- [Dashboard для Grafana](../admin/grafana_monitoring.md)
- [Управление плагинами](../dev/plugin_mgmt.md)
