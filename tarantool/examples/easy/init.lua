box.cfg({listen = 3301})

box.schema.func.create('easy', {language = 'C', if_not_exists = true})
box.schema.func.create('easy.easy2', {language = 'C', if_not_exists = true})

box.schema.user.grant('guest', 'execute', 'function', 'easy', {if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'function', 'easy.easy2', {if_not_exists = true})
