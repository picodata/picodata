package = 'test_app'
version = 'scm-1'
source  = {
    url = '/dev/null',
}
-- Put any modules your app depends on here
dependencies = {
    'tarantool',
    'lua >= 5.1',
    'checks == 3.3.0-1',
    'cartridge == 2.10.0-1',
    'metrics == 1.2.0-1',
    'crud == 1.5.2-1',
    'cartridge-cli-extensions == 1.1.2-1',
    'luatest == 1.0.1-1',
    'luacov == 0.13.0-1'
}
build = {
    type = 'none';
}
