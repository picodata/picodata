box.cfg()

-- You need to provide the module name and the name of that first proc.
-- Don't forget to provide a path to `liball_procs.so` in LUA_CPATH env var.
box.schema.func.create('all_procs.proc_names', { language = 'C' })

-- After that you can use 'all_procs.proc_names' to define the rest of the procs
local all_procs = box.func['all_procs.proc_names']:call()
for _, proc in pairs(all_procs) do
    box.schema.func.create(
        ('all_procs.%s'):format(proc), { language = 'C', if_not_exists = true }
    )
end

-- Now all the procs are defined
assert(box.func['all_procs.hello']:call() == 'hello')
assert(box.func['all_procs.add']:call{1, 2} == 3)

os.exit(0)
