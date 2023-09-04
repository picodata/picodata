local console = require("console")
local errno = require('errno')
local uri = require('uri')
local raw_uri = ...

local TIMEOUT_INFINITY = 100 * 365 * 86400
local opts = {}
opts.timeout = TIMEOUT_INFINITY

console.on_start(function(self)
    local status, reason
    status, reason = pcall(console.connect, raw_uri, opts)
    if not status then
        self:print(string.format("%s: %s", reason, errno.strerror()))
        -- Using urilib to exclude password
        self:print(string.format("uri: %s", uri.format(uri.parse(raw_uri), false)))
        os.exit(1)
    end
    -- We should only set this after we try to connect, because
    -- `console.connect` will call this before throwing eval errors
    console.on_client_disconnect(function(_)
        os.exit(0)
    end)
end)

return console.start()
