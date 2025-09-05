# Инструкция по смене адресов погашенного кластера

Ситуация: у вас есть набор директорий инстансов с .snap и .xlog файлами а также
файлами конфигурации, вам нужно сменить iproto и pgproto адреса, чтобы можно
было поднять этот кластер в другой сети.

Алгоритм примерно такой:

### 1. Сменить значения в таблице `_pico_peer_address`

В каждой instance_dir нужно при помощи `picodata tarantool` запустить вот такой скрипт:

```sh
picodata tarantool -- change-peer-addresses.lua
```

**change-peer-addresses.lua:**
```lua
local log = require 'log'
local yaml = require 'yaml'

box.cfg()

for _, row in box.space._pico_peer_address:pairs() do
    local base_port = 3300
    if row.connection_type == 'pgproto' then
        base_port = 5436
    end
    local address = ('0.0.0.0:%d'):format(base_port + row.raft_id)
    box.space._pico_peer_address:put{row.raft_id, address, row.connection_type}
end

local result = box.execute [[
    SELECT "name", "connection_type", "address" FROM "_pico_instance"
        JOIN "_pico_peer_address" ON "_pico_peer_address"."raft_id" = "_pico_instance"."raft_id"
]]

print("")
print("###########################################")
print("New listen addresses:")
print(yaml.encode(result.rows))

os.exit()
```

Этот скрипт сменит адреса инстансов всех инстансов в `_pico_peer_address` на
следующие значения:
    - iproto:  host = "0.0.0.0", port = 3300 + raft_id
    - pgproto: host = "0.0.0.0", port = 5436 + raft_id

А также выведет в конце новые значения.

### 2. Сменить значения в файлах конфигурации

В каждом файле конфигурации нужно сменить `listen`, `pg.listen` а также `peer`
на новые значения

Если файлов конфигурации нет, но есть логи инстансов, конфиг можно восстановить
по логам вручную.


### 3. Можно запускать

После этого кластер должен быть готов к запуску на новых адресах.
