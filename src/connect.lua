local console = require("console")
local uri = ...

local TIMEOUT_INFINITY = 100 * 365 * 86400
local opts = {}
opts.timeout = TIMEOUT_INFINITY

console.on_start(function(self)
    local status, reason
    status, reason = pcall(console.connect, uri, opts)
    if not status then
        -- `type(reason) == cdata`, so we have to convert it
        self:print(tostring(reason))
        os.exit(1)
    end
    -- We should only set this after we try to connect, because
    -- `console.connect` will call this before throwing eval errors
    console.on_client_disconnect(function(_)
        os.exit(0)
    end)
end)

return console.start()
