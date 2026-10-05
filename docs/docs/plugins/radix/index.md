# Общие сведения

В данном разделе приведены сведения о Radix, плагине для СУБД Picodata.
Информация в данном разделе дополняет пользовательскую документацию,
которая поставляется клиентам Picodata вместе со сборками плагина Radix.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Описание плагина {: #intro }

Radix — реализация [Redis](https://ru.wikipedia.org/wiki/Redis) на базе
Picodata, предназначенная для замены существующих инсталляций Redis.

Плагин Radix состоит из одноимённого сервиса (`radix`), реализующего
Redis на базе СУБД Picodata. Каждый экземпляр Radix открывает дополнительный
порт для подключения.

При использовании Picodata с плагином Radix нет необходимости в
отдельной инфраструктуре Redis Sentinel, так как каждый узел Picodata
выполняет роль прокси ко всем данным Redis.

## Соответствие версий Picodata и Radix {: #picodata_radix_versions }

Версии плагина Radix требуют определённых версий СУБД Picodata. Ниже
показана таблица совместимости версий:

| Radix  | Picodata        |  ФСТЭК-сертификат  |         LTS        |
|--------|-----------------|:------------------:|:------------------:|
| 0.9.0  | 25.2.2          | :white_check_mark: |                    |
| 0.14.1 | 25.5.7 (25.5.*) |                    |                    |
| 1.0.5  | 26.1.4 (26.1.*) | :white_check_mark: | :white_check_mark: |
| 1.0.8  | 26.1.6 (26.1.*) |                    | :white_check_mark: |
| 1.1.2  | 26.1.6 (26.1.*) |                    |                    |


См. также:

- [Страница загрузки Picodata](https://picodata.io/download/)

Основные разделы документации Radix:

<style>
.tiles-container {
    display: flex;
    flex-wrap: wrap;
    gap: 15px;
}

.tile {
    flex: 1 0 250px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.76);
    padding: 20px;
    border-radius: 10px;
    max-width: 30%;
    transition: transform 0.3s ease-in-out;
}

.tile:hover {
    transform: scale(1.05);
}

.tile h2 {
    font-size: 18px;
}

.tile a, .tile ul {
    font-size: 14px;
}
</style>

<main>
    <section class="tiles-container">
        <div class="tile">
            <h2>Начало работы</h2>
            <p><ul>
            <li><a href="install/">Установка</a></li>
            <li><a href="usage/">Подключение и работа с Radix</a></li>
            </ul></p>
        </div>
        <div class="tile">
            <h2>Настройка</h2>
            <p><ul>
            <li><a href="configuration/">Настройка плагина</a></li>
            <li><a href="radix_settings/">Описание конфигурации Radix</a></li>
            </ul></p>
        </div>
        <div class="tile">
            <h2>Справочная информация</h2>
            <p><ul>
            <li><a href="supported_commands/">Поддерживаемые команды</a></li>
            <li><a href="changelog/">Журнал изменений</a></li>
            </ul></p>
        </div>
    </section>
</main>
