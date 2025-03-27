# Дашборды grafana для Picodata

Официальная [документация](https://grafana.com/docs/grafana/latest/administration/provisioning/)

Picodata dashboard совместим с 8,9,10,11 версий графаны

## Provisioning для grafana dashboard

Нужно взять версию дашборда `Picodata-provisioning.json` и положить его в директорию `provisioning/dashboards/`

Положить файл `default.yml` в `provisioning/dashboards/` с содержимым:

```yaml
apiVersion: 1
providers:
  - name: default
    folder: ''
    type: file
    options:
      path: provisioning/dashboards
```

Также, опционально, можно добавить `datasource`, для этого положить файл `default.yaml` в `provisioning/datasources/` с содержимым (при этом сервер `prometheus` доступен по адресу http://localhost:9090):

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    orgId: 1
    url: http://localhost:9090
```

## Импорт дашборда в графану

Описано в нашей [документации](https://docs.picodata.io/picodata/stable/admin/monitoring/#grafana)