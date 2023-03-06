box.cfg({listen = 3301})

box.schema.func.create('tokio_hyper.start_server', {language = 'C', if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'function', 'tokio_hyper.start_server', {if_not_exists = true})

box.schema.space
    .create('fruit', {
        format = {{'id', 'unsigned'}, {'name', 'string'}, {'weight', 'number'}},
        if_not_exists = true,
    })
    :create_index('pk', { if_not_exists = true })

box.func['tokio_hyper.start_server']:call()

require 'console'.start()
