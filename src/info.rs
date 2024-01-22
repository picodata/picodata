use std::borrow::Cow;
use tarantool::proc;

pub const PICODATA_VERSION: &'static str = std::env!("GIT_DESCRIBE");
pub const PROC_API_VERSION: &'static str = "0.1.0";

////////////////////////////////////////////////////////////////////////////////
// VersionInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct VersionInfo<'a> {
    pub picodata_version: Cow<'a, str>,
    pub proc_api_version: Cow<'a, str>,
}

impl tarantool::tuple::Encode for VersionInfo<'_> {}

impl tarantool::proc::Return for VersionInfo<'_> {
    #[inline(always)]
    fn ret(self, ctx: tarantool::tuple::FunctionCtx) -> std::os::raw::c_int {
        tarantool::proc::ReturnMsgpack(self).ret(ctx)
    }
}

impl VersionInfo<'static> {
    #[inline(always)]
    pub fn current() -> Self {
        Self {
            picodata_version: PICODATA_VERSION.into(),
            proc_api_version: PROC_API_VERSION.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_version_info
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_version_info() -> VersionInfo<'static> {
    VersionInfo::current()
}
