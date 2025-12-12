---
search:
  exclude: true
---

# Добро пожаловать на портал документации Picodata

Picodata — это распределенная система промышленного уровня для
управления базами данных с возможностью расширения функциональности за
счет плагинов. Исходный код Picodata открыт и доступен как в нашем
[основном репозитории](https://git.picodata.io/core/picodata), так и в
[зеркале на GitHub](https://github.com/picodata/picodata).

Программное обеспечение Picodata реализует хранение структурированных и
неструктурированных данных, транзакционное управление данными, язык
запросов SQL, а также поддержку плагинов на языке Rust.

Основные разделы документации Picodata:

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
    transition: transform 0.3s ease-in-out;
}

.tile:hover {
    transform: scale(1.05);
}

.tile h2  {
  font-size: 18px;
}

.tile h3 {
  font-size: 16px;
}

.tile a, .tile ul {
  font-size: 14px;
}
</style>

<main>
    <section class="tiles-container">
        <div class="tile">
            <h2>Знакомство с Picodata</h2>
            <h3>Основные сведения</h3>
            <p><ul>
            <li><a href="overview/description/">Общее описание продукта</a></li>
            <li><a href="overview/benefits/">Преимущества Picodata</a></li>
            <li><a href="overview/versioning/">Версионирование</a></li>
            <li><a href="overview/licensing/">Лицензирование</a></li>
            </ul></p>
            <h3>Внутреннее устройство</h3>
            <p><ul>
            <li><a href="architecture/distributed_sql/">Распределенный SQL</a></li>
            <li><a href="architecture/topology_management/">Управление топологией</a></li>
            <li><a href="architecture/raft_failover/">Raft и отказоустойчивость</a></li>
            </ul></p>
        </div>
            <div class="tile">
            <h2>Первые шаги</h2>
            <h3>Установка и запуск</h3>
            <p><ul>
            <li><a href="tutorial/install/">Установка Picodata</a></li>
            <li><a href="tutorial/run/">Запуск Picodata</a></li>
            <li><a href="tutorial/connecting/">Подключение и работа в консоли</a></li>
            <li><a href="tutorial/webui/">Работа в веб-интерфейсе</a></li>
            </ul></p>
            <h3>Работа с кластером</h3>
            <p><ul>
            <li><a href="tutorial/deploy/">Создание кластера</a></li>
            <li><a href="tutorial/node_add/">Добавление</a> и <a href="tutorial/node_expel/">удаление узлов</a></li>
            <li><a href="tutorial/sql_examples/">Работа с данными SQL</a></li>
            </ul></p>
        </div>
        <div class="tile">
            <h2>Администрирование</h2>
            <h3>Развёртывание</h3>
            <p><ul>
            <li><a href="admin/deploy_ansible/">Развёртывание в Ansible</a></li>
            <li><a href="admin/deploy_kubernetes/">Развёртывание в Kubernetes</a></li>
            <li><a href="admin/server_setup/">Настройка серверов для кластера</a></li>
            </ul></p>
            <h3>Настройка и обслуживание</h3>
            <p><ul>
            <li><a href="admin/configure/">Способы конфигурирования</a></li>
            <li><a href="admin/monitoring/">Мониторинг</a></li>
            <li><a href="admin/troubleshooting/">Устранение неполадок</a></li>
            </ul></p>
        </div>
            <div class="tile">
            <h2>Разработка с Picodata</h2>
            <h3>Основные справочники</h3>
            <p><ul>
            <li><a href="sql_index/">Язык SQL</a></li>
            <li><a href="reference/cli/">Аргументы командной строки</a></li>
            <li><a href="dev/developer_tools">Инструментарий разработчика</a></li>
            <h3>Работа с плагинами</h3>
            <li><a href="architecture/plugins/">Механизм плагинов</a></li>
            <li><a href="dev/plugin_create/">Создание</a> и <a href="dev/plugin_mgmt/">управление плагинами</a></li>
            <li><a href="plugins/plugin_list">Обзор доступных плагинов</a></li>
            </ul></p>
        </div>
    </section>
</main>

<a style="display: none" href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fdocs.picodata.io%2Fpicodata%2F&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false"/></a>
