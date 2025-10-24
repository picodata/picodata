box.cfg({listen = 3301})

box.schema.func.create('async_h1_client.get', {language = 'C', if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'function', 'async_h1_client.get', {if_not_exists = true})

require 'console'.start()
