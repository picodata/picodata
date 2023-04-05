local console = require("console")
local TIMEOUT_INFINITY = 100 * 365 * 86400
local arg = ...

console.on_start(function(self)
    local status, reason
    status, reason = pcall(
        console.connect,
        arg,
        {connect_timeout = TIMEOUT_INFINITY}
    )
    if not status then
        self:print(reason)
        os.exit(0)
    end
end)

console.on_client_disconnect(function(_)
    os.exit(0)
end)

return console.start()
