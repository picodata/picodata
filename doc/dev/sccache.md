# Sccache in Picodata

## What is sccache?

Sccache is a compilation caching tool that significantly speeds up Rust project builds. It saves compilation results and reuses them for subsequent builds, which is especially valuable in CI/CD environments.

## Why do we need it?

During Picodata development, the same modules are compiled multiple times:
- When running tests in different configurations
- When checking code with linters
- When building in different profiles (debug/release)

Sccache avoids recompiling unchanged code, which:
- **Speeds up CI pipelines** — builds complete faster
- **Saves resources** — less CI runner time
- **Improves developer experience** — faster feedback on changes

## How it works in Picodata

### Automatic Integration

Sccache is automatically enabled in the CI environment and works transparently:

1. **For Rust code**: Cargo automatically uses sccache as a wrapper for rustc
2. **For C/C++ code**: The Tarantool build system automatically detects sccache and uses it for compilation

### Where is the cache stored?

The cache is stored in the `.sccache` directory within the project and is automatically preserved between CI job runs. Cache size is limited to 15GB for optimal balance between speed and disk usage.

### Supported Components

Sccache caches compilation for the following components:
- **Rust code**: All Picodata project crates
- **Tarantool**: Core database engine (C/C++)
- **HTTP server**: Built-in HTTP server
- **Dependencies**: External libraries on first compilation

## Local Development Usage

### Installation

```bash
cargo install sccache
```

### Configuration

Add to your shell profile (`.bashrc`, `.zshrc`, etc.):

```bash
# Enable sccache for Cargo
export RUSTC_WRAPPER=sccache

# Optional: configure cache size (default is 15GB)
export SCCACHE_CACHE_SIZE="15G"

# Optional: specify cache directory
export SCCACHE_DIR="$HOME/.cache/sccache"
```

### Verify it's working

After configuration, you can check cache statistics:

```bash
# Show statistics
sccache --show-stats

# Clear cache
sccache --clear-cache
```

When working correctly, you'll see cache hit statistics after repeated builds.

## Cache Management in CI

### Automatic Cleanup

The cache is automatically cleared on schedule to prevent bloat.
It is also possible to clear it manually in CI pipeline.

## Monitoring Effectiveness

In CI logs, you can see sccache statistics:
- **Compile requests**: Total number of compilation requests
- **Cache hits**: Number of successful cache hits
- **Cache misses**: Number of cache misses
- **Cache timeouts**: Timeouts when working with cache

## Additional Resources

- [Official sccache documentation](https://github.com/mozilla/sccache)
- [Rust compilation caching](https://doc.rust-lang.org/cargo/guide/build-cache.html)
