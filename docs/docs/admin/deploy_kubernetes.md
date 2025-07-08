# Picodata в Kubernetes

## Структура чарта Picodata {: #picodata_chart }

Чарт — это пакет [Helm] с набором ресурсов, необходимых для запуска
приложения, службы или любой другой программы внутри кластера Kubernetes.

[Helm]: https://helm.sh/

Структура чарта Picodata:

```shell
picodata/
  Chart.yaml            # >>> Файл с информацией о чарте
  values.yaml           # >>> Файл конфигурации чарта с параметрами по умолчанию
                        # (!) Содержит параметры кластера и инстанса для запуска
                        # Picodata
  templates/            # >>> Каталог шаблонов, из которых, в сочетании с
                        # параметрами из файла values.yaml, будут сгенерированы
                        # корректные файлы манифеста Kubernetes
    NOTES.txt           # ОПЦИОНАЛЬНО: Текстовый файл с короткими заметками
                        # по использованию чарта
    cm.yml              # ШАБЛОН: ConfigMap
                        # (!) Определяет файл конфигурации для каждого инстанса
                        # Picodata
    ingress.yml         # ШАБЛОН: Ingress
    picodata.yml        # ШАБЛОН: StatefulSet
    picodata-hpa.yml    # ШАБЛОН: HorizontalPodAutoscaler
    service.yml         # ШАБЛОН: Service
    serviceaccount.yml  # ШАБЛОН: ServiceAccount
    _helpers.tpl        # >>> Файл со вспомогательными функциями, используемыми
                        # в шаблонах
```

См. также Helm Docs:

* [Topics — The Chart File Structure](https://helm.sh/docs/topics/charts/#the-chart-file-structure)
* [Topics — The Chart.yaml File](https://helm.sh/docs/topics/charts/#the-chartyaml-file)
* [The Chart Best Practices Guide](https://helm.sh/docs/chart_best_practices/)
* [The Chart Template Developer's Guide](https://helm.sh/docs/chart_template_guide/)

### Состав тестового кластера {: #picodata_chart_cluster_composition }

Состав кластера Picodata в чарте определяется указанными в файле
[`values.yaml`] параметрами объекта `tiers`:

*  количество тиров — каждый со своим именем `tierName`
* `replicationFactor` — [фактор репликации] тира `tierName`
* `replicas` — общее количество подов Picodata, которое будет запущено
  в кластере Kubernetes

В чарте Picodata задан один тир, для которого указаны следующие значения:

```yaml title="values.yaml"
<...>
  tiers:
    - tierName: default
      replicationFactor: 1
      replicas: 2
      <...>
<...>
```

Иначе говоря, по умолчанию чарт Picodata создаст кластер с тиром *default*,
содержащим два репликасета — по одному инстансу на репликасет.

[`values.yaml`]: https://git.picodata.io/core/picodata-chart/-/blob/main/picodata/values.yaml
[фактор репликации]: ../reference/config.md#cluster_tier_tier_replication_factor

## Развертывание кластера Picodata в кластере Kubernetes {: #use_kubernetes }

Запустите кластер Kubernetes — например, с помощью пакета [Minikube]:

```shell
minikube start
```

[Minikube]: https://minikube.sigs.k8s.io/docs/

Склонируйте репозиторий [`picodata-chart`]:

```shell
git clone https://git.picodata.io/core/picodata-chart.git
```

[`picodata-chart`]: https://git.picodata.io/core/picodata-chart/

Перейдите в директорию, содержащую чарт Picodata:

```shell
cd picodata-chart/picodata/
```

С помощью менеджера пакетов [Helm] установите чарт Picodata в кластер
Kubernetes:

```shell
helm upgrade --install picodata -n picodata . --create-namespace
```

Проверьте статус подов `default-picodata-*` в кластере Kubernetes:

```shell
$ kubectl get pods -n picodata
NAME                 READY   STATUS    RESTARTS     AGE
default-picodata-0   1/1     Running   1 (9h ago)   9h
default-picodata-1   1/1     Running   1 (9h ago)   9h
```

## Проверка кластера Picodata {: #check_picodata_cluster }

### Протокол PostgreSQL (psql) {: #check_picodata_cluster_via_psql }

Пробросьте локальный порт 4327 к тому же порту пода `default-picodata-0`:

```shell
$ kubectl port-forward -n picodata default-picodata-0 4327
Forwarding from 127.0.0.1:4327 -> 4327
Forwarding from [::1]:4327 -> 4327
```

Откройте еще один терминал и [подключитесь](../tutorial/connecting.md#postgresql) к
инстансу Picodata в поде `default-picodata-0`:

```shell
psql postgres://admin:T0psecret@127.0.0.1:4327
```

Для подключения используется учетная запись администратора, пароль которой
задан переменной окружения `PICODATA_ADMIN_PASSWORD` в шаблоне чарта
Picodata [`templates/picodata.yml`].

[`templates/picodata.yml`]: https://git.picodata.io/core/picodata-chart/-/blob/main/picodata/templates/picodata.yml

Проверьте статус инстансов в кластере Picodata, прочитав системную таблицу
[`_pico_instance`]:

```shell
admin=> SELECT name, replicaset_name, current_state, tier
        FROM _pico_instance;
    name     | replicaset_name | current_state |  tier
-------------+-----------------+---------------+---------
 default_1_1 | default_1       | ["Online",1]  | default
 default_2_1 | default_2       | ["Online",1]  | default
(2 строки)
```

[`_pico_instance`]: ../architecture/system_tables.md#_pico_instance

### Веб-интерфейс (Web UI) {: #check_picodata_cluster_via_web_ui }

Пробросьте локальный порт 8081 к тому же порту пода `default-picodata-0`:

```shell
$ kubectl port-forward -n picodata default-picodata-0 8081
Forwarding from 127.0.0.1:8081 -> 8081
Forwarding from [::1]:8081 -> 8081
```

Откройте страницу [http://127.0.0.1:8081] для проверки статуса инстансов
в кластере Picodata.

[http://127.0.0.1:8081]: http://127.0.0.1:8081

См. также:

* [Инструкции и руководства ~ Работа в веб-интерфейсе](../tutorial/webui.md)

### Командная оболочка (Bash) {: #check_picodata_cluster_via_shell }

Подключитесь к командной оболочке Bash пода `default-picodata-0`:

```shell
kubectl exec -it default-picodata-0 -n picodata -- bash
```

Подключитесь [к консоли администратора] Picodata:

```shell
picodata admin admin.sock
```
[к консоли администратора]: ../tutorial/connecting.md#admin_console

Проверьте статус инстансов в кластере Picodata, прочитав системную
таблицу [`_pico_instance`]:

```
(admin) sql> SELECT name, replicaset_name, current_state, tier
             FROM _pico_instance;
+-------------+-----------------+---------------+---------+
| name        | replicaset_name | current_state | tier    |
+=========================================================+
| default_1_1 | default_1       | ["Online", 1] | default |
|-------------+-----------------+---------------+---------|
| default_2_1 | default_2       | ["Online", 1] | default |
+-------------+-----------------+---------------+---------+
(2 rows)
```

??? info "Дополнительные команды"

    Удалить чарт `picodata` и связанные с ним поды из пространства имен
    `picodata` в кластере Kubernetes:

    ```shell
    helm uninstall picodata -n picodata
    ```

    Вывести отладочный журнал для пода `default-picodata-0`:

    ```shell
    kubectl logs -n picodata default-picodata-0
    ```

    Вывести перечень событий для подов из пространства имен `picodata`:

    ```shell
    kubectl events -n picodata
    ```

    Вывести отладочный журнал для локального кластера Kubernetes:

    ```shell
    minikube logs
    ```

    Остановить локальный кластер Kubernetes:

    ```shell
    minikube stop
    ```

    Удалить локальный кластер Kubernetes:

    ```shell
    minikube delete
    ```
