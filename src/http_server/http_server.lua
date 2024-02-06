local json = require('json')

local function http_api_caller(fn_name)
    local ok, res = pcall(
	function()
	    return box.func[fn_name]:call({})
	end
    )

    local http_status = 200;
    local content_type = 'application/json';
    if ok == false then
        http_status = 500;
        content_type = 'text/plain';
    end

    return {
        status = http_status,
        body = json.encode(res),
        headers = {
            ['content-type'] = content_type,
        }
    }
end

local function http_api_cluster()
    return http_api_caller(".http_api_cluster")
end

local function http_api_tiers()
    return http_api_caller(".http_api_tiers")
end

local host, port = ...;
local httpd = require('http.server').new(host, port);
httpd:route({ method = 'GET', path = 'api/v1/tiers' }, http_api_tiers)
httpd:route({ method = 'GET', path = 'api/v1/cluster' }, http_api_cluster)
httpd:start();
_G.pico.httpd = httpd
