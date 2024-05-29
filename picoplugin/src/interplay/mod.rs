//! Communication between `picodata` runtime and any 3rtd party runtimes such `tokio`,
//! `smol` or even your custom runtime.
//! This module provides two API's:
//! 1) low level `cbus` API - with raw channels between custom runtime and `picodata` runtime
//! 2) high level `tros` API - with the ability to execute async tasks in 3rd party async runtimes
//! and wait for execution results.
