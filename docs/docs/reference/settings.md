---
hide:
  - toc
---

# Справочник настроек

В таблице ниже приведены все настройки Picodata, доступные для
изменения. Указаны способы изменения, значения по умолчанию и прочая
полезная информация.

Справочник разделен на две части:

- [настройки запуска Picodata](#picodata_start_settings) (параметры инстанса и кластера), управляемые через файл конфигурации и параметры CLI
- [настройки СУБД](#sql_settings), управляемые через SQL-запросы

<style>
.md-typeset .admonition.abstract {
    border-color: #9e9e9e;
}

.md-typeset .abstract > .admonition-title {
    background-color: #9e9e9e1a;
}

.md-typeset .abstract > .admonition-title::before {
    background-color: #9e9e9e;
}

.link {
    color:rgb(0, 0, 0) !important;
    text-decoration: none;
}

.sortable table  {

}

td {
    align-content: center;
    padding: 0.75em 0.5em !important;
    line-height: 1.3;
}

td.td3 {
    white-space: nowrap;
}

td.td3 ul {
    list-style-type: none;
}

.td3 ul,
.td3 ul > li {
    margin-left: 0 !important;
    word-break: break-all !important;
}

.tr-header td {
    font-size: 1.25em;
}

.tr-header > td > span {
    font-family: revert;
}

.tr-header > td.td3 {
    font-size: revert;
}

.center {
    text-align: center !important;
    width: auto;
}

.heading {
    text-align: center !important;
}

.legend-id {
    line-height: 2.5em;
    margin-left: 0.5em;
    width: 87em;
}

.legend-dash {
    margin: 0.5em; markdown="span"
}

.instance,
.cluster,
.sql-cluster,
.sql-tier {
    padding: 0.1em 0.5em;
    border-radius: 1em;
    font-family: monospace;
}

.instance {
    background-color: #d9ead3;
}

.absent {
    background-color: #f4cccc;
}

.cluster {
    background-color: #fff2cc;
}

.sql-cluster {
    background-color: #9fcaff;
}

.sql-tier {
    background-color: #6acc78;
}

.fill-width {
    width: 1000px;
}

.basic table {
    width: 100% !important;
    white-space: nowrap;
    table-layout: fixed !important;
    display: table;
    overflow: auto;
    overflow-x: hidden !important;
    font-size: .64rem;
    max-width: 100%;
    touch-action: auto;
}

.basic th, td {
    overflow-x: hidden !important;
    word-break: break-word;

}

/* Sortable tables */
table.sortable thead {
    background-color:#eee;
    color:#666666;
    cursor: default;
    margin:0;
    margin-bottom:.5em;
    padding:0 .8rem;
    white-space: nowrap;
    table-layout: fixed !important;
    overflow: auto;
    font-size: .64rem;
    overflow: auto !important;
    overflow-x: visible !important;
}

table.sortable {
    border-width: 0.1em !important;
    border-style: solid;
    width: 87em;
}

table.sortable tbody tr:nth-child(2n) td {
    font-size: .64rem;
    background-color:var(--md-typeset-table-color--light);
}

table.sortable tbody tr:nth-child(2n+1) td {
    font-size: .64rem;
    background-color:var(--md-default-bg-color);
}

.container{
  display: block;
  overflow-x: auto;
}

table.legend {
    border-collapse:separate;
    border:solid #9e9e9e 1px;
    border-radius:10px;
    width: 87em;
}

</style>

## Настройки запуска Picodata {: #picodata_start_settings }

### Легенда {: #legend_picodata data-search-exclude }

<table class="legend"><tr><td>
    <span class="instance legend-id">audit</span><span class="legend-dash">—</span>настройка применима к отдельному инстансу<br>
    <span class="cluster legend-id">cluster-name</span><span class="legend-dash">—</span>настройка применима ко всему кластеру<br>
</td></tr></table>
<br>
<div markdown="span" class="container">
<table markdown="span" class="sortable">
    <thead>
        <tr>
            <th class="heading" style="width:15%"><button>Название</button></th>
            <th class="heading" style="width:20%"><button>Описание</button></th>
            <th class="heading" style="width:5%"><button>Значение <br> по умолчанию</button></th>
            <th class="heading" style="width:20%"><button>CLI</button></th>
            <th class="heading"><button>Файл конфигурации</button></th>
            <th class="heading" style="width:20%"><button>Переменная</button></th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><span class="instance">—</span></td>
            <td>Пароль администратора для текущего инстанса</td>
            <td>null</td>
            <td>
            `export PICODATA_ADMIN_PASSWORD='T0psecret'`
            </td>
            <td>
            </td>
            <td>PICODATA_ADMIN_PASSWORD</td>
        </tr>
        <tr>
            <td><span class="instance">admin-sock</span></td>
            <td>Путь к unix-сокету для подключения к консоли администратора</td>
            <td>admin.sock в рабочей директории инстанса</td>
            <td>[picodata run --admin-sock](cli.md#run_admin_sock)</td>
            <td>[instance.admin_socket](config.md#instance_admin_socket)</td>
            <td>PICODATA_ADMIN_SOCK</td>
        </tr>
        <tr>
            <td><span class="instance">audit</span></td>
            <td>Конфигурация журнала аудита</td>
            <td>null</td>
            <td>[picodata run --audit](cli.md#run_audit)</td>
            <td>[instance.audit](config.md#instance_audit)</td>
            <td>PICODATA_AUDIT_LOG</td>
        </tr>
        <tr>
            <td><span class="instance">auth-type</span></td>
            <td>Метод аутентификации</td>
            <td>md5</td>
            <td>[picodata expel --auth-type](cli.md#expel_auth_type)</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">boot_timeout</span></td>
            <td>Время, в течение которого инстанс ожидает загрузки перед присоединением к кластеру (с)</td>
            <td>7200</td>
            <td>[picodata run -c instance.boot_timeout=3600](cli.md#run_config_parameter)</td>
            <td>[instance.boot.timeout](config.md#instance_boot_timeout)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="cluster">bucket_count</span></td>
            <td>Число сегментов в тире</td>
            <td>3000</td>
            <td></td>
            <td>[cluster.tier.<tier_name\>.bucket_count](config.md#cluster_tier_tier_bucket_count)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="cluster">config</span></td>
            <td>Путь к файлу конфигурации в формате YAML</td>
            <td>null</td>
            <td>[picodata run --config](cli.md#run_config)</td>
            <td></td>
            <td>PICODATA_CONFIG_FILE</td>
        </tr>
        <tr>
            <td><span class="instance">config-parameter</span></td>
            <td>Список пар ключ-значение, определяющий параметры конфигурации</td>
            <td>null</td>
            <td>[picodata run --config-parameter](cli.md#run_config_parameter)</td>
            <td></td>
            <td>PICODATA_CONFIG_PARAMETERS</td>
        </tr>
        <tr>
            <td><span class="cluster">default_bucket_count</span></td>
            <td>Число сегментов в кластере по умолчанию</td>
            <td>3000</td>
            <td></td>
            <td>[cluster.default_bucket_count](config.md#cluster_default_bucket_count)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">failure-domain</span></td>
            <td>Список пар ключ-значение, разделенных запятыми, определяющий географическое расположение сервера</td>
            <td>{}</td>
            <td>[picodata run --failure-domain](cli.md#run_failure_domain)</td>
            <td>[instance.failure_domain](config.md#instance_failure_domain)</td>
            <td>PICODATA_FAILURE_DOMAIN</td>
        </tr>
        <tr>
            <td><span class="instance">http-listen</span></td>
            <td>Адрес HTTP-сервера</td>
            <td>null</td>
            <td>[picodata run --http-listen](cli.md#run_http_listen)</td>
            <td>[instance.http_listen](config.md#instance_http_listen)</td>
            <td>PICODATA_HTTP_LISTEN</td>
        </tr>
        <tr>
            <td><span class="cluster">init-replication-factor</span></td>
            <td>Число реплик (инстансов с одинаковым набором хранимых данных) для каждого репликасета</td>
            <td>1</td>
            <td>[picodata run --init-replication-factor](cli.md#run_init_replication_factor)</td>
            <td>[cluster.default_replication_factor](config.md#cluster_tier_tier_replication_factor)</td>
            <td>PICODATA_INIT_REPLICATION_FACTOR</td>
        </tr>
        <tr>
            <td><span class="instance">instance-dir</span></td>
            <td>Рабочая директория инстанса</td>
            <td>./</td>
            <td>[picodata run --instance-dir](cli.md#run_instance_dir)</td>
            <td>[instance.instance_dir](config.md#instance_instance_dir)</td>
            <td>PICODATA_INSTANCE_DIR</td>
        </tr>
        <tr>
            <td><span class="instance">instance-name</span></td>
            <td>Имя инстанса</td>
            <td>null</td>
            <td>[picodata run --instance-name](cli.md#run_instance_name)</td>
            <td>[instance.name](config.md#instance_instance_dir)</td>
            <td>PICODATA_INSTANCE_NAME</td>
        </tr>
        <tr>
            <td><span class="instance">iproto-advertise</span></td>
            <td>Публичный сетевой адрес инстанса</td>
            <td>127.0.0.1:3301</td>
            <td>[picodata run --iproto-advertise](cli.md#run_iproto_advertise)</td>
            <td>[instance.iproto_advertise](config.md#instance_iproto_advertise)</td>
            <td>PICODATA_IPROTO_ADVERTISE</td>
        </tr>
        <tr>
            <td><span class="instance">iproto-listen</span></td>
            <td>Сетевой адрес инстанса</td>
            <td>127.0.0.1:3301</td>
            <td>[picodata run --iproto-listen](cli.md#run_iproto_listen)</td>
            <td>[instance.iproto_listen](config.md#instance_iproto_listen)</td>
            <td>PICODATA_IPROTO_LISTEN</td>
        </tr>
        <tr>
            <td><span class="cluster">iproto-tls.enabled</span></td>
            <td>Признак использования аутентификации по сертификату при доступе к инстансу (mutual TLS, mTLS)</td>
            <td>false</td>
            <td></td>
            <td>[instance.iproto_tls.enabled](config.md#instance_iproto_tls)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">iproto-tls.cert_file</span></td>
            <td>Путь к файлу серверного сертификата для использования с mTLS</td>
            <td></td>
            <td></td>
            <td>[instance.iproto_tls.cert_file](config.md#instance_iproto_tls)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">iproto-tls.key_file</span></td>
            <td>Путь к файлу закрытого ключа для использования с mTLS</td>
            <td></td>
            <td></td>
            <td>[instance.iproto_tls.key_file](config.md#instance_iproto_tls)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="cluster">iproto-tls.ca_file</span></td>
            <td>Путь к файлу корневого сертификата для использования с mTLS</td>
            <td></td>
            <td></td>
            <td>[instance.iproto_tls.ca_file](config.md#instance_iproto_tls)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">log</span></td>
            <td>Конфигурация отладочного журнала</td>
            <td>null</td>
            <td>[picodata run --log](cli.md#run_log)</td>
            <td>[instance.log.destination](config.md#instance_log_destination)</td>
            <td>PICODATA_LOG</td>
        </tr>
        <tr>
            <td><span class="instance">log-level</span></td>
            <td>Уровень важности событий, регистрируемых в отладочном журнале</td>
            <td>info</td>
            <td>[picodata run --log-level](cli.md#run_log_level)</td>
            <td>[instance.log.level](config.md#instance_log_level)</td>
            <td>PICODATA_LOG_LEVEL</td>
        </tr>
        <tr>
            <td><span class="instance">memtx-memory</span></td>
            <td>Объем оперативной памяти в байтах, используемый движком хранения memtx</td>
            <td>67108864</td>
            <td>[picodata run --memtx-memory](cli.md#run_memtx_memory)</td>
            <td>[instance.memtx.memory](config.md#instance_memtx_memory)</td>
            <td>PICODATA_MEMTX_MEMORY</td>
        </tr>
        <tr>
            <td><span class="instance">memtx-system-memory</span></td>
            <td>Объем оперативной памяти в байтах, используемый движком хранения memtx для системных таблиц</td>
            <td>268435456</td>
            <td>[picodata run --memtx-memory](cli.md#run_memtx_memory)</td>
            <td>[instance.memtx.system_memory](config.md#instance_memtx_system_memory)</td>
            <td>PICODATA_MEMTX_SYSTEM_MEMORY</td>
        </tr>
        <tr>
            <td><span class="instance">memtx.max_tuple_size</span></td>
            <td>Максимальный размер кортежа в байтах для движка хранения memtx</td>
            <td>1048576</td>
            <td>[picodata run -c instance.memtx.max_tuple_size=2M](cli.md#run_config_parameter)</td>
            <td>[instance.memtx.max_tuple_size](config.md#instance_memtx_max_tuple_size)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">password-file</span></td>
            <td>Путь к файлу с паролем указанного пользователя</td>
            <td>null</td>
            <td>[picodata expel --password-file](cli.md#expel_password_file)</td>
            <td></td>
            <td>PICODATA_PASSWORD_FILE</td>
        </tr>
        <tr>
            <td><span class="instance">peer</span></td>
            <td>Список сетевых адресов других инстансов, разделенных запятыми</td>
            <td>- 127.0.0.1:3301</td>
            <td>
            [picodata run --peer](cli.md#run_peer)<br>
            [picodata expel --peer](cli.md#expel_peer)<br>
            [picodata status --peer](cli.md#status_peer)
            </td>
            <td>
            [instance.peer](config.md#instance_peer)
            </td>
            <td>PICODATA_PEER</td>
        </tr>
        <tr>
            <td><span class="instance">pg-advertise</span></td>
            <td>Публичный адрес сервера для подключения по протоколу PostgreSQL</td>
            <td>127.0.0.1:4327</td>
            <td>[picodata run --pg-advertise](cli.md#run_pg_advertise)</td>
            <td>[instance.pg.advertise](config.md#instance_pg_advertise)</td>
            <td>PICODATA_PG_ADVERTISE</td>
        </tr>
        <tr>
            <td><span class="instance">pg-listen</span></td>
            <td>Адрес сервера для подключения по протоколу PostgreSQL</td>
            <td>127.0.0.1:4327</td>
            <td>[picodata run --pg-listen](cli.md#run_pg_listen)</td>
            <td>[instance.pg.listen](config.md#instance_pg_listen)</td>
            <td>PICODATA_PG_LISTEN</td>
        </tr>
        <tr>
            <td><span class="instance">pg.ssl</span></td>
            <td>Признак использования протокола SSL при подключении к SQL-консоли</td>
            <td>false</td>
            <td>[picodata run -c instance.pg.ssl=true](cli.md#run_config_parameter)</td>
            <td>[instance.pg.ssl](config.md#instance_pg_ssl)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">replicaset-name</span></td>
            <td>Имя репликасета. Используется при инициализации кластера и присоединении инстанса к уже существующему кластеру</td>
            <td>null</td>
            <td>[picodata run --replicaset-name](cli.md#run_replicaset_name)</td>
            <td>[instance.replicaset_name](config.md#instance_replicaset_name)</td>
            <td>PICODATA_REPLICASET_NAME</td>
        </tr>
        <tr>
            <td><span class="instance">service-password-file</span></td>
            <td>Путь к текстовому файлу с паролем для системного пользователя pico_service</td>
            <td>null</td>
            <td>
            [picodata plugin configure --service-password-file](cli.md#plugin_configure_service_password_file)<br>
            [picodata status --service-password-file](cli.md#status_service_password_file)
            </td>
            <td></td>
            <td>PICODATA_SERVICE_PASSWORD_FILE</td>
        </tr>
        <tr>
            <td><span class="instance">script</span></td>
            <td>Путь к файлу Lua-скрипта, который будет выполнен после присоединения инстанса к кластеру</td>
            <td>null</td>
            <td>[picodata run --script](cli.md#run_script)</td>
            <td></td>
            <td>PICODATA_SCRIPT</td>
        </tr>
        <tr>
            <td><span class="instance">share-dir</span></td>
            <td>Путь к директории, содержащей файлы плагинов</td>
            <td>null</td>
            <td>[picodata run --share-dir](cli.md#run_share_dir)</td>
            <td>[instance.share_dir](config.md#instance_share_dir)</td>
            <td>PICODATA_SHARE_DIR</td>
        </tr>
        <tr>
            <td><span class="cluster">shredding</span></td>
            <td>Режим безопасного удаления рабочих файлов инстанса</td>
            <td>false</td>
            <td>[picodata run --shredding](cli.md#run_shredding)</td>
            <td>[cluster.shredding](config.md#cluster_shredding)</td>
            <td>PICODATA_SHREDDING</td>
        </tr>
        <tr>
            <td><span class="instance">tier</span></td>
            <td>Имя тира, которому будет принадлежать инстанс</td>
            <td>default</td>
            <td>[picodata run --tier](cli.md#run_tier)</td>
            <td>[instance.tier](config.md#instance_tier)</td>
            <td>PICODATA_SHREDDING</td>
        </tr>
        <tr>
            <td><span class="cluster">tls-auth</span></td>
            <td>Признак использования аутентификации по сертификату при доступе к инстансу, на
            котором включен и настроен mTLS. При значении `true` необходимо
            использовать остальные параметры mTLS (т.е. указать путь к клиентскому
            сертификату, закрытому клиентскому ключу и корневому сертификату).</td>
            <td>false</td>
            <td>
            [picodata expel --tls-auth](cli.md#tls_auth)<br>
            [picodata plugin configure --tls-auth](cli.md#tls_auth)<br>
            [picodata status --tls-auth](cli.md#tls_auth)
            </td>
            <td></td>
            <td>PICODATA_IPROTO_TLS_AUTH</td>
        </tr>
        <tr>
            <td><span class="cluster">tls-ca</span></td>
            <td>Путь к файлу корневого сертификата для доступа к инстансу, на котором
            включен и настроен mTLS.</td>
            <td></td>
            <td>
            [picodata expel --tls-ca](cli.md#tls_ca)<br>
            [picodata plugin configure --tls-ca](cli.md#tls_ca)<br>
            [picodata status --tls-ca](cli.md#tls_ca)
            </td>
            <td></td>
            <td>PICODATA_IPROTO_TLS_CA</td>
        </tr>
        <tr>
            <td><span class="instance">tls-cert</span></td>
            <td>Путь к файлу клиентского сертификата для доступа к инстансу, на котором
            включен и настроен mTLS.</td>
            <td></td>
            <td>
            [picodata expel --tls-cert](cli.md#tls_cert)<br>
            [picodata plugin configure --tls-cert](cli.md#tls_cert)<br>
            [picodata status --tls-cert](cli.md#tls_cert)
            </td>
            <td></td>
            <td>PICODATA_IPROTO_TLS_CERT</td>
        </tr>
        <tr>
            <td><span class="instance">tls-key</span></td>
            <td>
            Путь к файлу закрытого клиентского ключа для доступа к инстансу, на котором
            включен и настроен mTLS.</td>
            <td></td>
            <td>
            [picodata expel --tls-key](cli.md#tls_key)<br>
            [picodata plugin configure --tls-key](cli.md#tls_key)<br>
            [picodata status --tls-key](cli.md#tls_key)
            </td>
            <td></td>
            <td>PICODATA_IPROTO_TLS_KEY</td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.bloom_fpr</span></td>
            <td>Вероятность ложноположительного срабатывания фильтра Блума для движка хранения vinyl, измеряемая в долях единицы</td>
            <td>0.05</td>
            <td>[picodata run -c instance.vinyl.bloom_fpr=0.10](cli.md#run_config_parameter)</td>
            <td>[instance_vinyl_bloom_fpr](config.md#instance_vinyl_bloom_fpr)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.cache</span></td>
            <td>Размер кэша в байтах для движка хранения vinyl</td>
            <td>134217728</td>
            <td>[picodata run -c instance.vinyl.cache=256M](cli.md#run_config_parameter)</td>
            <td>[instance_vinyl_cache](config.md#instance_vinyl_cache)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.max_tuple_size</span></td>
            <td>Максимальный размер кортежа в байтах для движка хранения vinyl</td>
            <td>1048576</td>
            <td>[picodata run -c instance.vinyl.max_tuple_size=2M](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.max_tuple_size](config.md#instance_vinyl_max_tuple_size)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.memory</span></td>
            <td>Максимальное количество оперативной памяти в байтах, которое использует движок хранения vinyl</td>
            <td>134217728</td>
            <td>[picodata run -c instance.vinyl.memory=256M](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.memory](config.md#instance_vinyl_memory)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.page_size</span></td>
            <td>Размер страницы в байтах, используемой движком хранения vinyl для операций чтения и записи на диск</td>
            <td>8192</td>
            <td>[picodata run -c instance.vinyl.page_size=16M](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.page_size](config.md#instance_vinyl_page_size)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.range_size</span></td>
            <td>Максимальный размер LSM-поддерева по умолчанию в байтах для движка хранения vinyl</td>
            <td>1073741824</td>
            <td>[picodata run -c instance.vinyl.range_size=2G](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.range_size](config.md#instance_vinyl_range_size)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.read_threads</span></td>
            <td>Максимальное количество потоков чтения для движка хранения vinyl</td>
            <td>1</td>
            <td>[picodata run -c instance.vinyl.run_count_per_level=4](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.read_threads](config.md#instance_vinyl_read_threads)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.run_size_ratio</span></td>
            <td>Соотношение между размерами разных уровней в LSM-дереве для движка хранения vinyl</td>
            <td>3.5</td>
            <td>[picodata run -c instance.vinyl.run_size_ratio=7.0](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.run_size_ratio](config.md#instance_vinyl_run_size_ratio)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.timeout</span></td>
            <td>Максимальное время обработки запроса движком хранения vinyl в секундах</td>
            <td>60.0</td>
            <td>[picodata run -c instance.vinyl.timeout=120.0](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.timeout](config.md#instance_vinyl_timeout)</td>
            <td></td>
        </tr>
        <tr>
            <td><span class="instance">vinyl.write_threads</span></td>
            <td>Максимальное количество потоков записи для движка хранения vinyl</td>
            <td>4</td>
            <td>[picodata run -c instance.vinyl.write_threads=8](cli.md#run_config_parameter)</td>
            <td>[instance.vinyl.write_threads](config.md#instance_vinyl_write_threads)</td>
            <td></td>
        </tr>
    </tbody>
</table>
</div>

## Настройки СУБД {: #sql_settings }

### Легенда {: #legend_db data-search-exclude }

<table class="legend"><tr><td>
    <span class="sql-cluster legend-id">pg_portal_max</span><span class="legend-dash">—</span>настройка применима к кластеру<br>
    <span class="sql-tier legend-id">iproto_net_msg_max</span><span class="legend-dash">—</span>настройка применима к отдельному тиру<br>
</td></tr></table>
<br>

<div markdown="span" class="container">
<table markdown="span" class="sortable">
    <thead>
        <tr>
            <th style="width:30%"><button>Название</button></th>
            <th style="width:30%"><button>Описание</button></th>
            <th style="width:10%"><button>Значение <br> по умолчанию</button></th>
            <th style="width:10%"><button>Пример SQL-команды</button></th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><span class="sql-cluster">[auth_login_attempt_max](db_config.md#auth_login_attempt_max){.link}</span></td>
            <td>Максимальное количество неуспешных попыток аутентификации через `picodata connect`</td>
            <td>4</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_login_attempt_max = 5;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[auth_password_enforce_digits](db_config.md#auth_password_enforce_digits){.link}</span></td>
            <td>Признак пароля пользователя, требующий наличия хотя бы одной цифры</td>
            <td>true</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_password_enforce_digits to false;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[auth_password_enforce_specialchars](db_config.md#auth_password_enforce_specialchars){.link}</span></td>
            <td>Признак пароля пользователя, требующий наличия хотя бы одного спецсимвола</td>
            <td>false</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_password_enforce_specialchars to true;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[auth_password_enforce_lowercase](db_config.md#auth_password_enforce_lowercase){.link}</span></td>
            <td>Признак пароля пользователя, требующий наличия хотя бы одного символа в нижнем регистре</td>
            <td>true</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_password_enforce_lowercase to false;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[auth_password_enforce_uppercase](db_config.md#auth_password_enforce_uppercase){.link}</span></td>
            <td>Признак пароля пользователя, требующий наличия хотя бы одного символа в верхнем регистре</td>
            <td>true</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_password_enforce_uppercase to false;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[auth_password_length_min](db_config.md#auth_password_length_min){.link}</span></td>
            <td>Минимальная длина пароля, требуемая при установке или изменении пароля пользователя в Picodata</td>
            <td>8</td>
            <td>
            ```sql
            ALTER SYSTEM SET auth_password_length_min = 9;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[governor_auto_offline_timeout](db_config.md#governor_auto_offline_timeout){.link}</span></td>
            <td>Время в секундах, после которого инстанс, не отвечающий на запросы от raft-лидера, будет автоматически переведен в состояние Offline</td>
            <td>30.0</td>
            <td>
            ```sql
            ALTER SYSTEM SET governor_auto_offline_timeout = 40.1;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[governor_common_rpc_timeout](db_config.md#governor_common_rpc_timeout){.link}</span></td>
            <td>Время ожидания до перехода губернатора к новой итерации цикла, за которое инстансы должны ответить губернатору на его RPC-запросы</td>
            <td>3.0</td>
            <td>
            ```sql
            ALTER SYSTEM SET governor_common_rpc_timeout = 4.0;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[governor_plugin_rpc_timeout](db_config.md#governor_plugin_rpc_timeout){.link}</span></td>
            <td>Время ожидания до перехода губернатора к новой итерации цикла, за которое инстансы должны ответить губернатору на его RPC-запросы для настройки системы плагинов</td>
            <td>10.0</td>
            <td>
            ```sql
            ALTER SYSTEM SET governor_plugin_rpc_timeout = 20;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[governor_raft_op_timeout](db_config.md#governor_raft_op_timeout){.link}</span></td>
            <td>Время ожидания до перехода губернатора к новой итерации цикла, за которое предложенные губернатором изменения в raft-журнал должны быть применены к локальной raft-машине</td>
            <td>3.0</td>
            <td>
            ```sql
            ALTER SYSTEM SET governor_raft_op_timeout = 4.0;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-tier">[iproto_net_msg_max](db_config.md#iproto_net_msg_max){.link}</span></td>
            <td>Максимальное количество сообщений, которое Picodata обрабатывает параллельно</td>
            <td>0x300</td>
            <td>
            ```sql
            ALTER SYSTEM SET iproto_net_msg_max = 0x400 FOR TIER default;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-tier">[memtx_checkpoint_count](db_config.md#memtx_checkpoint_count){.link}</span></td>
            <td>Максимальное количество снапшотов, хранящихся в директории `memtx_dir`</td>
            <td>2</td>
            <td>
            ```sql
            ALTER SYSTEM SET memtx_checkpoint_count = 200 FOR TIER default;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-tier">[memtx_checkpoint_interval](db_config.md#memtx_checkpoint_interval){.link}</span></td>
            <td>Период активности службы создания снапшотов (checkpoint daemon) в секундах</td>
            <td>3600.0</td>
            <td>
            ```sql
            ALTER SYSTEM SET memtx_checkpoint_interval= 7200.0 FOR TIER default;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[pg_portal_max](db_config.md#pg_portal_max){.link}</span></td>
            <td>Размер хранилища порталов PostgreSQL</td>
            <td>1024</td>
            <td>
            ```sql
            ALTER SYSTEM SET pg_portal_max = 2048;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[pg_statement_max](db_config.md#pg_statement_max){.link}</span></td>
            <td>Размер хранилища стейтментов PostgreSQL</td>
            <td>1024</td>
            <td>
            ```sql
            ALTER SYSTEM SET pg_statement_max = 2048;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[raft_snapshot_chunk_size_max](db_config.md#raft_snapshot_chunk_size_max){.link}</span></td>
            <td>Максимальный размер фрагмента в raft-снапшоте в байтах</td>
            <td>16777216</td>
            <td>
            ```sql
            ALTER SYSTEM SET raft_snapshot_chunk_size_max = 16777217;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[raft_snapshot_read_view_close_timeout](db_config.md#raft_snapshot_read_view_close_timeout){.link}</span></td>
            <td>Время в секундах, после которого окно чтения (read view) снапшота будет принудительно закрыто</td>
            <td>86400</td>
            <td>
            ```sql
            ALTER SYSTEM SET raft_snapshot_read_view_close_timeout = 86500;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[raft_wal_count_max](db_config.md#raft_wal_count_max){.link}</span></td>
            <td>Максимальное количество записей в raft-журнале, при превышении которого он будет автоматически компактизирован</td>
            <td>64</td>
            <td>
            ```sql
            ALTER SYSTEM SET raft_wal_count_max = 128;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[raft_wal_size_max](db_config.md#raft_wal_size_max){.link}</span></td>
            <td>Максимальный размер raft-журнала в байтах, при превышении которого он будет автоматически компактизирован</td>
            <td>67108864</td>
            <td>
            ```sql
            ALTER SYSTEM SET raft_wal_size_max = 67108865;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[sql_storage_cache_count_max](db_config.md#sql_storage_cache_count_max){.link}</span></td>
            <td>Максимальное количество prepared statement-ов для размещения в LRU-кэше на узлах хранения Vshard</td>
            <td>50</td>
            <td>
            ```sql
            ALTER SYSTEM SET sql_storage_cache_count_max = 60;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[sql_storage_cache_size_max](db_config.md#sql_storage_cache_size_max){.link}</span></td>
            <td>Динамический параметр Box API, регулирующий размер LRU-кэша в байтах</td>
            <td>5242880</td>
            <td>
            ```sql
            ALTER SYSTEM SET sql_storage_cache_size_max = 15242880;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[sql_motion_row_max](db_config.md#sql_motion_row_max){.link}</span></td>
            <td>Максимальное количество строк в виртуальной таблице, собирающей результаты отдельных локальных запросов</td>
            <td>5000</td>
            <td>
            ```sql
            ALTER SYSTEM SET sql_motion_row_max = 10000;
            ```
            </td>
        </tr>
        <tr>
            <td><span class="sql-cluster">[sql_vdbe_opcode_max](db_config.md#sql_vdbe_opcode_max){.link}</span></td>
            <td>Максимальное количество команд при исполнении локального плана с помощью VDBE на узле кластера</td>
            <td>45000</td>
            <td>
            ```sql
            ALTER SYSTEM SET sql_vdbe_opcode_max = 90000;
            ```
            </td>
        </tr>
    </tbody>
</table>
</div>
