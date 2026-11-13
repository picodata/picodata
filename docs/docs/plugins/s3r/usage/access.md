# Управление доступом

Страница описывает, как владелец [бакета](../reference/glossary.md#bucket)
открывает и ограничивает доступ к бакету и объектам через S3 API: списки
управления доступом, политику бакета, блокировку публичного доступа, заранее
подписанные ссылки и правила CORS. Она нужна разработчику клиентского
приложения. Правила CORS, которые [тенант](../reference/glossary.md#tenant) получает по умолчанию, задаёт администратор (см.
[Правила CORS тенанта по умолчанию](../admin/tenants.md#default_cors)).

Решение о доступе складывается из трёх источников:

- [список управления доступом](#acl) и [политика бакета](#bucket_policy)
  разрешают доступ независимо друг от друга;
- явный запрет в политике действует всегда;
- флаги [блокировки публичного доступа](#public_access_block) имеют приоритет
  над списками и политикой.

Разделы идут в порядке: кому разрешён доступ, как его ограничить, доступ без
ключа, доступ из браузера. Каждый раздел содержит описание, пример и перечень
особенностей и ограничений.

## Списки управления доступом {: #acl }

Список управления доступом (ACL) перечисляет, кому и что разрешено делать с
бакетом или объектом. Поддерживаются предопределённые списки (`private`,
`public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-read`,
`bucket-owner-full-control`) и явные разрешения.

```shell
aws s3api put-object-acl --bucket documents --key report.pdf --acl public-read
aws s3api get-object-acl --bucket documents --key report.pdf

# Явное разрешение отдельному пользователю
aws s3api put-object-acl --bucket documents --key report.pdf \
    --grant-read 'id=bob'
```

**Особенности и ограничения**

- Идентификатором получателя разрешения служит имя пользователя Picodata.
- Группы задаются идентификаторами URI
  `http://acs.amazonaws.com/groups/global/AllUsers` и
  `http://acs.amazonaws.com/groups/global/AuthenticatedUsers`.
- Владелец объекта и доступ по умолчанию описаны в разделе
  [Права доступа к объектам](#objects_access_mgmt).

### Права доступа к объектам {: #objects_access_mgmt }

Владельцем объекта становится пользователь, который его создал. Владелец бакета
прав на объект не получает; такое поведение определено спецификацией S3.

По умолчанию объект доступен только владельцу и не хранит списка управления
доступом (ACL), поэтому при чтении такого объекта список не используется.

Доступ другим пользователям открывают
[списки управления доступом](#acl). Поддерживаются:

- предопределённые списки: `private`, `public-read`, `public-read-write`,
  `authenticated-read`, `bucket-owner-read`, `bucket-owner-full-control`;
- явные разрешения для отдельных пользователей;
- явные разрешения для групп «все пользователи» и «все аутентифицированные
  пользователи».

## Политика бакета {: #bucket_policy }

Политика служит вторым источником разрешений, независимым от списков управления
доступом. Явный запрет действует всегда; если запрета нет, разрешение даёт либо
политика, либо список управления доступом.

```shell
cat > policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam:::user/bob"},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::documents/public/*"]
     },
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": "arn:aws:s3:::documents/secret/*"
     }
  ]
 }
EOF

aws s3api put-bucket-policy --bucket documents --policy file://policy.json
aws s3api get-bucket-policy --bucket documents
aws s3api get-bucket-policy-status --bucket documents
aws s3api delete-bucket-policy --bucket documents
```

Поддерживаемые элементы политики:

| Элемент | Что поддерживается |
| --- | --- |
| `Principal` | имя пользователя Picodata без ARN; ARN вида `arn:aws:iam::<учётная-запись>:user/<имя>`, в котором значима последняя составляющая; `"*"` |
| `Action`, `NotAction` | с подстановочными знаками `*` и `?` |
| `Resource`, `NotResource` | с подстановочными знаками `*` и `?` |
| Операторы условий | `StringEquals`, `StringNotEquals`, `StringLike`, `StringNotLike` и суффикс `IfExists` |
| Ключи условий | `s3:prefix`, `s3:ExistingObjectTag/<ключ>`, `s3:RequestObjectTag/<ключ>` и любой ключ вида `s3:x-amz-<заголовок>`, то есть заголовок запроса с этим именем, например `s3:x-amz-acl`, `s3:x-amz-copy-source` и `s3:x-amz-metadata-directive` |

**Особенности и ограничения**

- [Субъект доступа](../reference/glossary.md#principal) задаётся именем
  пользователя Picodata.
- Значение `"Principal": "*"` разрешает анонимное обращение.
- Имя ключа условия сравнивается без учёта регистра. Ключ тега после
  `s3:ExistingObjectTag/` и `s3:RequestObjectTag/` сравнивается с учётом
  регистра, как ключи тегов в S3.
- При загрузке формой из браузера (`POST Object`) ключ `s3:x-amz-<заголовок>`
  читается из поля формы с этим именем, а ключ `s3:RequestObjectTag/<ключ>` из
  поля `x-amz-tagging`; заголовки самого запроса `POST` в условиях не
  учитываются.
- Условие, которое плагин не может вычислить, трактуется в пользу отказа. Это
  условие с оператором, который плагин не реализует, или с ключом, которого нет
  в таблице выше, например `aws:username`, `aws:SourceIp` или `s3:VersionId`.
  Инструкция `Allow` с таким условием не даёт доступа, а инструкция `Deny`
  запрещает его. `PutBucketPolicy` принимает политику с таким условием без
  ошибки, поэтому единственным признаком такого условия служит отказ `AccessDenied`
  при обращении.
- Запрос с параметром `versionId` проверяется по действию над версией объекта:
  `s3:GetObjectVersion`, `s3:DeleteObjectVersion`,
  `s3:GetObjectVersionTagging`, `s3:PutObjectVersionTagging`,
  `s3:DeleteObjectVersionTagging`, `s3:GetObjectVersionAcl`,
  `s3:PutObjectVersionAcl` или `s3:GetObjectVersionAttributes`. Тот же запрос
  без `versionId` проверяется по действию над текущим объектом, например
  `s3:GetObject`. Поэтому запрет действия над текущим объектом не
  распространяется на запрос с указанной версией, в том числе на запрос с
  `versionId=null` к бакету без версионирования. Чтобы запретить изменение
  тегов, политика называет оба действия: `s3:PutObjectTagging` и
  `s3:PutObjectVersionTagging`.
- Запрос `GetObjectAttributes` проверяется только по действию
  `s3:GetObjectAttributes` или `s3:GetObjectVersionAttributes`; в AWS он
  проверяется также по действию `s3:GetObject` или `s3:GetObjectVersion`.

## Блокировка публичного доступа {: #public_access_block }

Блокировка публичного доступа состоит из четырёх флагов бакета, которые имеют
приоритет над списками управления доступом и политикой.

```shell
aws s3api put-public-access-block --bucket documents \
    --public-access-block-configuration \
    'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'

aws s3api get-public-access-block --bucket documents
aws s3api delete-public-access-block --bucket documents
```

| Флаг | Действие |
| --- | --- |
| `BlockPublicAcls` | отклоняет запись списка управления доступом, который даёт доступ группам «все пользователи» и «все аутентифицированные пользователи» |
| `IgnorePublicAcls` | сохраняет такие разрешения, но не учитывает их при проверке |
| `BlockPublicPolicy` | отклоняет запись политики с `"Principal": "*"` без условий |
| `RestrictPublicBuckets` | отменяет для запросов без подписи разрешения публичной политики; её запреты остаются в силе |

**Особенности и ограничения**

- `PutPublicAccessBlock` заменяет настройку целиком: опущенный флаг означает
  `false`, а не сохранение прежнего значения.
- Запрос `GetPublicAccessBlock` для бакета, где настройка не задавалась,
  возвращает `NoSuchPublicAccessBlockConfiguration` (404). Настройка из четырёх
  значений `false` считается заданной и читается обратно.
- Различие первых двух флагов существенно при разборе инцидента.
  `BlockPublicAcls` не даёт публичному разрешению появиться. `IgnorePublicAcls`
  оставляет разрешение в списке и перестаёт его учитывать, поэтому
  `GetBucketAcl` и `GetObjectAcl` по-прежнему его показывают, а снятие флага
  возвращает разрешение в силу.

## Заранее подписанные ссылки и анонимный доступ {: #presigned_links }

Доступ к объекту без ключа доступа дают два способа:
[заранее подписанная ссылка](../reference/glossary.md#presigned_url) и
анонимное обращение.

Ссылка, подпись которой передаётся в параметрах запроса, даёт доступ к объекту
тому, у кого нет ключа доступа:

```shell
aws s3 presign s3://documents/report.pdf --expires-in 3600
```

Анонимное обращение разрешает либо предопределённый список управления доступом
`public-read`, либо политика бакета с `"Principal": "*"`:

```shell
aws s3api put-object-acl --bucket documents --key public/logo.png --acl public-read
curl https://s3.example.net/documents/public/logo.png
```

**Особенности и ограничения**

- Флаги [блокировки публичного доступа](#public_access_block) имеют приоритет
  над списком управления доступом и политикой, которые разрешают анонимное
  обращение.

## Совместное использование ресурсов между источниками (CORS) {: #resources_sharing }

Правила Cross-Origin Resource Sharing (CORS) определяют, каким источникам
браузер разрешит обращаться к бакету. Без правил браузер отклоняет ответ до
того, как его получит приложение.

```shell
cat > cors.json <<'EOF'
{
  "CORSRules": [
    {
      "AllowedMethods": ["GET", "PUT"],
      "AllowedOrigins": ["https://app.example.com", "https://*.example.com"],
      "AllowedHeaders": ["x-amz-*"],
      "ExposeHeaders": ["ETag", "x-amz-request-id"],
      "MaxAgeSeconds": 3000
     }
  ]
 }
EOF

aws s3api put-bucket-cors --bucket documents --cors-configuration file://cors.json
aws s3api get-bucket-cors --bucket documents
aws s3api delete-bucket-cors --bucket documents
```

**Особенности и ограничения**

- Допустимы методы `GET`, `PUT`, `POST`, `DELETE` и `HEAD` и не более
  100 правил.
- Документ проверяется при записи: неизвестный метод или образец с несколькими
  символами `*` отклоняются.
- Заголовки `Access-Control-*` добавляются и к обычным ответам, в том числе к
  ответам с ошибкой: без них браузер не покажет приложению даже код ошибки.

### Образец источника {: #cors_origin_pattern }

Образец источника содержит не более одного символа `*` и сопоставляется со
значением целиком. Примеры:

- `*suffix` совпадает с `foo.suffix`, но не с `foo.suffix.get`;
- `start*end` совпадает с `startend` и `start12end`, но не с `0start12end`.

Одиночный `*` разрешает любой источник, и в ответе возвращается именно `*`, а не
имя источника, поэтому браузер кэширует такой ответ для всех источников сразу.

### Предварительный запрос {: #cors_preflight }

Предварительный запрос (`OPTIONS`) обслуживается без подписи, потому что
браузер её не присылает, и не обращается к объекту. Применяется первое правило,
у которого совпали источник, метод и все заголовки из
`Access-Control-Request-Headers`. Правило, в котором запрошенный заголовок не
перечислен, не считается совпавшим.

| Предварительный запрос | Ответ |
| --- | --- |
| Правило совпало, в том числе для несуществующего ключа | `200` |
| У бакета нет правил или ни одно правило не совпало | `403` без заголовков `Access-Control-*`; браузер не отправляет основной запрос |
| Нет заголовка `Origin` или заголовка `Access-Control-Request-Method` | `400` |
