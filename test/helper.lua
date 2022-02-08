local log = require('log')
local helper = table.copy(require('luatest.helpers'))

log.cfg({log_level = 6})
helper.Picodata = require('test.helper.picodata')

return helper
