# Инструментарий разработчика {: #developer_tools }

На этой странице собраны ссылки на материалы, востребованные при
разработке программного обеспечения для Picodata.

## Внешние коннекторы {: #drivers }

Доступные коннекторы:

- [Java](../dev/connectors/java.md)
- [JDBC](../dev/connectors/jdbc.md)
- [Go](../dev/connectors/go.md)

## Разработка плагинов {: #plugins }

Мы предоставляем [руководство по разработке плагина] для Picodata на языке
Rust, с примерами кода и указаниями по запуску. Отдельно доступна
[статья], рассказывающая об управлении жизненном циклом плагина в
кластере.

[руководство по разработке плагина]: plugin_create.md
[статья]: plugin_mgmt.md

## Dashboard для Grafana {: #grafana }

Для загрузки доступен dashboard-файл [Picodata.json] для веб-интерфейса
Grafana. Инструкция по использованию dashboard-файла и настройке мониторинга
приведена в разделе [Dashboard для Grafana].

[Picodata.json]: https://git.picodata.io/core/picodata/-/tree/master/monitoring/dashboard
[Dashboard для Grafana]: ../admin/grafana_monitoring.md

## Роль Picodata для Ansible {: #ansible }

Роль Picodata для Ansible доступна в Git-репозитории
[picodata-ansible](https://git.picodata.io/core/picodata-ansible)

Для установки роли выполните команду:

```shell
ansible-galaxy install -f git+https://git.picodata.io/core/picodata-ansible.git
```

Создание инвентарного файла, плейбука, а также доступный набор действий
с ролью описаны в разделе [Развертывание кластера через
Ansible](../admin/deploy_ansible.md)

## Helm-чарт Picodata {: #helm }

Helm-чарт для Picodata доступен в Git-репозитории
[picodata-chart](https://git.picodata.io/core/picodata-chart)

Для установки чарта выполните команды:

```shell
git clone https://git.picodata.io/core/picodata-chart.git
cd picodata/
helm upgrade --install picodata -n picodata . --create-namespace
```

Более подробно использование чарта для развертывания кластера Picodata в контейнерной
среде описано в разделе [Picodata в Kubernetes](../admin/deploy_kubernetes.md)
