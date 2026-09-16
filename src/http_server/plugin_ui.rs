use super::auth::{auth_middleware_optional, AuthContext};
use super::{as_admin, ApiError, ApiResult, HttpResponse, HttpResponseTable};
use crate::config::PicodataConfig;
use crate::plugin::PluginIdentifier;
use crate::schema::WebuiPageAuth;
use crate::storage::Catalog;
use crate::tlog;
use crate::traft;
use ::tarantool::tlua;
use http::StatusCode;
use serde::Serialize;
use smol_str::SmolStr;
use std::path::{Component, Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////
// GET /api/v1/plugin-ui/pages
////////////////////////////////////////////////////////////////////////////////

/// A single entry of the `GET /api/v1/plugin-ui/pages` response
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PluginUiPage {
    plugin: SmolStr,
    slug: SmolStr,
    title: SmolStr,
    entry: SmolStr,
    version: SmolStr,
    auth: WebuiPageAuth,
}

fn http_api_plugin_ui_pages(auth_ctx: Option<AuthContext>) -> traft::Result<Vec<PluginUiPage>> {
    let storage = Catalog::get();

    // already sorted by name
    let enabled_plugins = storage.plugins.all_enabled()?;

    let mut pages = Vec::new();
    for plugin_def in enabled_plugins {
        let plugin = plugin_def.name;
        let version = plugin_def.version;

        for page in plugin_def.webui {
            if auth_ctx.is_none() && page.auth != WebuiPageAuth::Plugin {
                continue;
            }

            pages.push(PluginUiPage {
                plugin: plugin.clone(),
                slug: page.slug,
                title: page.title,
                entry: page.entry,
                version: version.clone(),
                auth: page.auth,
            });
        }
    }

    Ok(pages)
}

pub(crate) fn http_api_plugin_ui_pages_with_auth(
    auth_header: String,
) -> ApiResult<Vec<PluginUiPage>> {
    as_admin(|| auth_middleware_optional(auth_header, http_api_plugin_ui_pages))
}

////////////////////////////////////////////////////////////////////////////////
// GET /plugin-ui/<plugin>/<version>/*path
////////////////////////////////////////////////////////////////////////////////

const ASSET_DIR: [&str; 2] = ["assets", "webui"];

/// `true` if `rel_path` is a plain relative path with no `..`/`.`/root
/// components, i.e. it cannot escape the directory it's joined onto.
fn is_safe_relative_path(rel_path: &str) -> bool {
    if rel_path.is_empty() {
        return false;
    }

    Path::new(rel_path)
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
}

fn resolve_asset_path(plugin: &str, version: &str, rel_path: &str) -> Option<PathBuf> {
    if !is_safe_relative_path(rel_path) {
        return None;
    }

    let share_dir = PicodataConfig::get().instance.share_dir();
    let mut path = share_dir.join(plugin).join(version);
    path.extend(ASSET_DIR);
    path.push(rel_path);

    path.is_file().then_some(path)
}

/// Guess the `Content-Type` of a plugin webui asset from its file extension.
///
/// Only text-based formats are supported: just like the core WebUI bundle
/// (see `webui/vite.config.js`'s `generateBuildFolder`), asset bodies travel
/// through Lua strings and thus must be valid UTF-8.
fn guess_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "application/javascript",
        Some("css") => "text/css",
        Some("json") | Some("map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("txt") => "text/plain; charset=utf-8",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn not_found_response() -> HttpResponse {
    ApiError::NotFound(String::from("asset not found")).into()
}

/// Serve a single file from `share_dir/<plugin>/<version>/assets/webui/<rel_path>`.
fn serve_plugin_asset(plugin: &str, version: &str, rel_path: &str) -> HttpResponse {
    let Some(path) = resolve_asset_path(plugin, version, rel_path) else {
        return not_found_response();
    };

    let Ok(bytes) = std::fs::read(&path) else {
        return not_found_response();
    };

    let Ok(body) = String::from_utf8(bytes) else {
        return crate::traft::error::Error::other("asset is not valid UTF-8").into();
    };

    HttpResponse::to_response(StatusCode::OK, guess_content_type(&path), body)
}

fn plugin_ui_route_name(ident: &PluginIdentifier) -> String {
    format!("plugin_ui:{}:{}", ident.name, ident.version)
}

/// Register the static asset route `/plugin-ui/<plugin>/<version>/*` for a
/// just-enabled plugin. Idempotent: any previously registered route under the
/// same name is replaced.
///
/// Must be called only once the plugin is fully enabled (all `on_start`
/// callbacks succeeded), see [`crate::plugin::manager::PluginManager::try_enable`],
/// which treats a registration failure here as fatal to the enable as a whole -
/// a plugin that declares webui pages but can't actually serve them isn't
/// considered successfully enabled.
pub(crate) fn register_plugin_ui_route(ident: &PluginIdentifier) -> Result<(), String> {
    let lua = ::tarantool::lua_state();
    let route_name = plugin_ui_route_name(ident);
    let route_path = format!("/plugin-ui/{}/{}/*path", ident.name, ident.version);
    let plugin = ident.name.to_string();
    let version = ident.version.to_string();

    // deregister first, so we do not fail on name collision
    lua.exec_with(
        r#"
        local route_name, route_path, handler = ...
        pico.httpd:delete(route_name)
        pico.httpd:route({method = 'GET', path = route_path, name = route_name}, function(req)
            local rel_path = req:stash('path') or ''
            return handler(rel_path)
        end)
        "#,
        (
            route_name.clone(),
            route_path,
            tlua::Function::new(move |rel_path: String| -> HttpResponseTable {
                serve_plugin_asset(&plugin, &version, &rel_path).into()
            }),
        ),
    )
    .map_err(|err| err.to_string())
}

/// Remove the static asset route registered by [`register_plugin_ui_route`]
/// for `ident`. A no-op if no such route is currently registered.
pub(crate) fn unregister_plugin_ui_route(ident: &PluginIdentifier) {
    let lua = ::tarantool::lua_state();
    let route_name = plugin_ui_route_name(ident);

    let result = lua.exec_with(
        "local route_name = ...; pico.httpd:delete(route_name)",
        route_name.clone(),
    );

    if let Err(err) = result {
        tlog!(
            Error,
            "failed to remove webui route `{route_name}` for plugin `{ident}`: {err}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_relative_path_accepts_plain_paths() {
        assert!(is_safe_relative_path("dist/page.js"));
        assert!(is_safe_relative_path("page.js"));
        assert!(is_safe_relative_path("icons/dashboard.svg"));
    }

    #[test]
    fn safe_relative_path_rejects_traversal_and_absolute() {
        assert!(!is_safe_relative_path(""));
        assert!(!is_safe_relative_path("../secret"));
        assert!(!is_safe_relative_path("dist/../../secret"));
        assert!(!is_safe_relative_path("/etc/passwd"));
        assert!(!is_safe_relative_path("./page.js"));
    }

    #[test]
    fn content_type_guessed_from_extension() {
        assert_eq!(
            guess_content_type(Path::new("a.js")),
            "application/javascript"
        );
        assert_eq!(guess_content_type(Path::new("a.css")), "text/css");
        assert_eq!(guess_content_type(Path::new("a.json")), "application/json");
        assert_eq!(guess_content_type(Path::new("a.svg")), "image/svg+xml");
        assert_eq!(
            guess_content_type(Path::new("a.unknown")),
            "application/octet-stream"
        );
    }
}
