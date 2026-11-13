# Настройки и удаление бакета

Страница описывает настройки [бакета](../reference/glossary.md#bucket),
которые задаёт пользователь хранилища через S3 API: версионирование, блокировку
объектов, теги и правила жизненного цикла, а также удаление бакета. Она нужна
разработчику клиентского приложения. Как кластер выполняет правила жизненного
цикла и очищает удалённый бакет, описано на странице
[Фоновые процессы](../concepts/background.md#lifecycle_execution). Настройки доступа к
бакету описаны на странице [Управление доступом](access.md).

Разделы идут в порядке: защита данных от перезаписи и удаления, теги,
автоматическое удаление, удаление бакета. Каждый раздел содержит описание,
пример и перечень особенностей и ограничений.

## Версионирование объекта {: #object_versioning }

[Версионирование](../reference/glossary.md#versioning) включается для бакета.
После включения каждая запись создаёт новую версию с собственным `VersionId`,
а чтение без указания версии возвращает последнюю.

!!! warning "Версионирование нельзя выключить"
    Выключить версионирование после включения нельзя, можно только
    приостановить. Удаление с указанием версии удаляет данные безвозвратно.

```shell
aws s3api put-bucket-versioning --bucket documents \
    --versioning-configuration Status=Enabled
aws s3api get-bucket-versioning --bucket documents
```

```shell
aws s3api list-object-versions --bucket documents --prefix report

# Прочитать конкретную версию
aws s3api get-object --bucket documents --key report.pdf \
    --version-id 0000019874f3a1c2 old.pdf

# Удалить конкретную версию: данные удаляются безвозвратно
aws s3api delete-object --bucket documents --key report.pdf \
    --version-id 0000019874f3a1c2
```

**Особенности и ограничения**

- Удаление без указания версии не удаляет данные, а создаёт
  [маркер удаления](../reference/glossary.md#delete_marker). Ответ на такое
  удаление содержит заголовки `x-amz-delete-marker: true` и `x-amz-version-id`
  созданного маркера.
- Состояние `Suspended` останавливает создание новых версий: запись заменяет
  версию с идентификатором `null`, а прежние версии сохраняются и остаются
  доступны по `VersionId`.

### Ответы на запросы к версиям и маркерам удаления {: #versioning_responses }

| Запрос | Ответ |
| --- | --- |
| `GET`, `HEAD`, `GetObjectAttributes`, `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging`, `GetObjectAcl` или `PutObjectAcl` без указания версии, когда последней версией ключа служит маркер удаления | `NoSuchKey` (404) с заголовком `x-amz-delete-marker: true`; запросы чтения передают также идентификатор маркера в `x-amz-version-id` |
| `versionId` указывает на сам маркер удаления | `MethodNotAllowed` (405) с заголовками `x-amz-delete-marker: true`, `x-amz-version-id` с идентификатором маркера и `Allow: DELETE`; ответ на запрос чтения содержит также `Last-Modified` маркера |
| `versionId` правильного формата, но у ключа нет такой версии | `NoSuchVersion` (404) |
| `versionId` неправильного формата | `NoSuchKey` (404) |

По идентификатору из `x-amz-version-id` маркер удаления можно удалить и тем
самым вернуть предыдущую версию. Маркер удаления, записанный в состоянии
`Suspended`, служит версией `null`, и заголовок `x-amz-version-id` для него не
передаётся.

## Блокировка объектов {: #object_block }

Блокировка объектов запрещает удалять версию объекта до истечения срока
хранения или до снятия юридического удержания.

!!! warning "Срок хранения зависит от часов узлов"
    Срок хранения сравнивается с системными часами [узлов](../reference/glossary.md#node). Если часы отстают
    или переведены назад, срок хранения в режиме `COMPLIANCE` истечёт раньше
    назначенного. На узлах хранилища требуется синхронизация времени.

Блокировка включается только при создании бакета и требует версионирования.

```shell
aws s3api create-bucket --bucket vault --object-lock-enabled-for-bucket

# Режим и срок хранения по умолчанию для новых объектов
aws s3api put-object-lock-configuration --bucket vault \
    --object-lock-configuration \
    'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=30}}'

# Срок хранения отдельной версии
aws s3api put-object-retention --bucket vault --key contract.pdf \
    --retention 'Mode=COMPLIANCE,RetainUntilDate=2027-01-01T00:00:00Z'

# Юридическое удержание без срока
aws s3api put-object-legal-hold --bucket vault --key contract.pdf \
    --legal-hold Status=ON
```

**Особенности и ограничения**

- Блокировка защищает версии, а не ключи. Пока срок хранения версии не истёк
  или на ней установлено юридическое удержание, удаление этой версии
  отклоняется с ответом `AccessDenied`.
- Запись новой версии поверх заблокированной и создание маркера удаления при
  этом разрешены.
- Режим `COMPLIANCE` не обходится никем, включая владельца бакета.
- Юридическое удержание снимается запросом `PutObjectLegalHold` со значением
  `Status=OFF` и требует права `s3:PutObjectLegalHold`.

### Обход режима `GOVERNANCE` {: #governance_bypass }

Обход запрашивается заголовком `x-amz-bypass-governance-retention: true` и
требует права `s3:BypassGovernanceRetention`.

```shell
aws s3api delete-object --bucket vault --key contract.pdf \
    --version-id 0000019874f3a1c2 --bypass-governance-retention
```

| Действие | Без обхода | С обходом режима `GOVERNANCE` |
| --- | --- | --- |
| Удаление версии в режиме `GOVERNANCE` | отклоняется | разрешено |
| Удаление версии в режиме `COMPLIANCE` | отклоняется | отклоняется |
| Сокращение срока в режиме `GOVERNANCE` | отклоняется | разрешено |
| Продление срока в любом режиме | разрешено | разрешено |
| Смена режима `GOVERNANCE` на `COMPLIANCE` | отклоняется | разрешено |
| Смена режима `COMPLIANCE` на `GOVERNANCE` | отклоняется | отклоняется |
| Удаление версии с юридическим удержанием | отклоняется | отклоняется |

## Теги {: #tags }

Теги представляют собой пары из ключа и значения, которые задаются для бакета и для объекта.

```shell
aws s3api put-object-tagging --bucket documents --key report.pdf \
    --tagging 'TagSet=[{Key=class,Value=confidential}]'
aws s3api get-object-tagging --bucket documents --key report.pdf
aws s3api delete-object-tagging --bucket documents --key report.pdf
```

**Особенности и ограничения**

- И у бакета, и у объекта может быть не более 10 тегов.
- Теги объекта используются в [правилах жизненного цикла](#lifecycle_rules) и в
  условиях [политики бакета](access.md#bucket_policy)
  (`s3:ExistingObjectTag/<ключ>`, `s3:RequestObjectTag/<ключ>`).

## Правила жизненного цикла {: #lifecycle_rules }

Правило жизненного цикла определяет, какие объекты и когда удаляются или
переносятся в другой [класс хранения](../reference/glossary.md#storage_class).

!!! warning "Объект может быть удалён позже срока правила"
    Срок в правиле задаёт самый ранний момент удаления, а не точный. При массовом
    истечении срока удаление растягивается во времени
    (см. [Как выполняются правила](../concepts/background.md#lifecycle_execution)). Настройки темпа
    удаления нет; задержку относительно срока показывает метрика
    `s3r_lifecycle_lateness_seconds`.

```shell
cat > lifecycle.json <<'EOF'
{
  "Rules": [
    {
      "ID": "expire-logs",
      "Status": "Enabled",
      "Filter": {"Prefix": "logs/"},
      "Expiration": {"Days": 30}
     },
    {
      "ID": "tidy-versions",
      "Status": "Enabled",
      "Filter": {},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
     },
    {
      "ID": "large-temp",
      "Status": "Enabled",
      "Filter": {
        "And": {
          "Prefix": "tmp/",
          "ObjectSizeGreaterThan": 10485760,
          "Tags": [{"Key": "class", "Value": "scratch"}]
         }
       },
      "Expiration": {"Days": 1}
     }
  ]
 }
EOF

aws s3api put-bucket-lifecycle-configuration --bucket documents \
    --lifecycle-configuration file://lifecycle.json
aws s3api get-bucket-lifecycle-configuration --bucket documents
aws s3api delete-bucket-lifecycle --bucket documents
```

Поддерживаются следующие действия:

- `Expiration` по числу дней `Days`, по дате `Date` и с параметром
  `ExpiredObjectDeleteMarker`;
- `NoncurrentVersionExpiration`, в том числе с параметром
  `NewerNoncurrentVersions`;
- `AbortIncompleteMultipartUpload`;
- переходы между классами хранения `Transition` и
  `NoncurrentVersionTransition`.

Фильтр правила задаётся префиксом `Prefix`, тегом `Tag`, границами размера
`ObjectSizeGreaterThan` и `ObjectSizeLessThan` либо их сочетанием через `And`.

**Особенности и ограничения**

- Конфигурация проверяется при записи, и ошибка в ней возвращает ответ
  `InvalidArgument`.
- Срок `Days` отсчитывается от создания объекта и округляется вверх до
  ближайшей полуночи UTC, как в AWS.
- Дата `Date` принимается с точностью до секунды и исполняется в указанный
  момент, тогда как AWS требует, чтобы дата указывала на полночь.
- Если объект попадает под действующее правило удаления, ответы `GET` и `HEAD`
  содержат заголовок `x-amz-expiration` с датой удаления и идентификатором
  правила. Значение вычисляется по правилам при каждом запросе и после
  изменения правил сразу отражает новые сроки.

### Переходы между классами хранения {: #lifecycle_transitions }

Переход выполняется только в класс с более редким доступом: класс назначения
должен находиться в [карте классов](../reference/glossary.md#class_map) ниже текущего, иначе конфигурация отклоняется
с ответом `InvalidArgument`.

Что происходит с объектом при переходе, зависит от того, где хранится его
содержимое:

| Объект | Переход |
| --- | --- |
| Содержимое хранится на дисках | Данные переписываются в [группу размещения](../reference/glossary.md#placement_group) [яруса](../reference/glossary.md#storage_tier) нового класса. До завершения переноса объект читается из прежнего размещения |
| Содержимое хранится в строке метаданных | Меняется только метка класса |
| Любой объект в кластере с единственным ярусом | Меняется только метка класса; классом назначения может быть любое имя класса AWS |

## Удаление бакета {: #bucket_deletion }

Операция `DeleteBucket` удаляет только пустой бакет.

```shell
aws s3api delete-bucket --bucket documents
```

**Особенности и ограничения**

- Если в бакете есть объект, версия или маркер удаления, операция отвечает
  `BucketNotEmpty`.
- После успешного ответа новые запросы к бакету, включая запросы к его
  настройкам, получают ответ `NoSuchBucket`, а `ListBuckets` и консоль бакет не
  показывают. Так отвечает каждый инстанс, который применил пометку об
  удалении.
- Повторный `DeleteBucket` отвечает `NoSuchBucket`.
- Запись, которая шла одновременно с удалением, либо завершается успешно, и её
  объект удаляется вместе с бакетом, либо получает ответ `NoSuchBucket`.
- Имя бакета освобождается не сразу: пока идёт очистка, создание бакета с тем
  же именем отвечает `OperationAborted` (`409`). После очистки имя снова
  свободно.

Как кластер очищает строки удалённого бакета, описано в разделе
[Очистка после удаления бакета](../concepts/background.md#bucket_deletion_cleanup).
