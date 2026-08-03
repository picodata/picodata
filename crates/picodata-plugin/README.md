# picodata-plugin

[![Crates.io](https://img.shields.io/crates/v/picodata-plugin)](https://crates.io/crates/picodata-plugin)
[![Documentation](https://docs.rs/picodata-plugin/badge.svg)](https://docs.rs/picodata-plugin/25.1.1)
[![License](https://img.shields.io/badge/license-BSD--2--Clause-blue)](LICENSE)


Framework for building Picodata plugins in Rust.

## Installation

```toml
[dependencies]
picodata-plugin = "25.1.1"
```

## Quick Start
That is basically a "Hello, World" plugin. A full walkthrough on a plugin creation can be found [here](https://docs.picodata.io/picodata/devel/tutorial/create_plugin/).  

```rust
impl Service for PluginService {
    type Config = Option<config::Config>;

    fn on_config_change(
        &mut self,
        _ctx: &PicoContext,
        _new_config: Self::Config,
        _old_config: Self::Config,
    ) -> CallbackResult<()> {
        Ok(())
    }

    fn on_start(&mut self, _context: &PicoContext, _config: Self::Config) -> CallbackResult<()> {
        Ok(())
    }

    fn on_stop(&mut self, _context: &PicoContext) -> CallbackResult<()> {
        Ok(())
    }

    fn on_leader_change(&mut self, _context: &PicoContext) -> CallbackResult<()> {
        Ok(())
    }
}
```

## Resources
[Example Plugin](https://docs.picodata.io/picodata/devel/tutorial/create_plugin/)
[API Documentation](https://docs.rs/picodata-plugin/latest/picodata_plugin/)

## License
This project is licensed under the BSD-2-Clause license. See [LICENSE](https://git.picodata.io/core/picodata/-/blob/master/LICENSE?ref_type=heads) for details.
