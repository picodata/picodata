This should really be automated.

# How do I update these?

The urls for the tarballs come from `tarantool-sys/static-build/CMakeLists.txt`
(not the patched one, but from upstream tarantool).

Whenever there's a bump in the dependecies:
- download the needed archives
- remove the extra bloat that we don't need (see below for list of what to remove)
- update the `tarantool-patches/0006-build-vendor-the-dependencies.patch` with the
    correct paths
- remove old versions of dependecies
- commit

# What to remove from dependecies?

The dependencies have a lot of extra stuff like tests, docs, etc. that we don't
use but which take up a lot of space.
So we remove anything we can before commiting.
The original commit with dependecies trimming is
`3b5e2dc83e232d6cc05b51cc80319f25785a7535 chore(build): trim some fat`.
Here's a list of directories, we currently remove.

### vendor/icu4-62*
- source/samples
- source/test
- also remove the corresponding file paths from
  - source/Makefile.in
  - source/configure

### vendor/libiconv-1.*
- tests

### vendor/ncurses-6.*
- Ada95
- doc
- man
- test
- also remove the corresponding file paths from
  - configure

### vendor/openssl-1.1.1*
- apps
- doc
- test
