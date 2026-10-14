# @picodata/ui-kit

SDK types and design tokens for building Picodata WebUI plugins. The package
provides the public SDK contract and the host theme's colors, sizes, shadows,
and fonts without a dependency on React or MUI.

## Installation

Configure the Picodata package registry in your project's `.npmrc`:

```ini
@picodata:registry=${NPM_REGISTRY_URL}
```

Set `NPM_REGISTRY_URL` to the npm repository URL provided by the Picodata Nexus
administrator.

Install the package:

```bash
npm install @picodata/ui-kit
```

For Yarn 4, configure `.yarnrc.yml` instead:

```yaml
npmScopes:
  picodata:
    npmRegistryServer: "${NPM_REGISTRY_URL}"
```

```bash
yarn add @picodata/ui-kit
```

If the registry requires authentication, configure a token with package read
access through an environment variable and scope it to the registry URL.

## Quick Start

Export an `HTMLElement` class as the page entry point:

```ts
import type { WebuiPageElement, WebuiSdk } from "@picodata/ui-kit";

export default class PluginPage
  extends HTMLElement
  implements WebuiPageElement
{
  webuiSdk!: WebuiSdk;

  connectedCallback() {
    this.textContent = this.webuiSdk.getLocale();
  }
}
```

The host registers the class and assigns `element.webuiSdk` before mounting the
element. Read the SDK in `connectedCallback`, not in the constructor. Release
subscriptions and other page resources in `disconnectedCallback`.

The SDK provides the current theme, locale, API base URL, and session token.
`getSessionToken()` reads the token synchronously; `refreshSession()` asks the
host to refresh it and returns a promise. Refresh failures expose an error code:
`SESSION_EXPIRED` or `REFRESH_FAILED`. Use `reportFatalError()` to report an
unrecoverable page error to the host. See [index.d.ts](index.d.ts) for the full
contract.

## Design Tokens

The host includes `tokens.css` at document level. CSS custom properties are
inherited into a plugin's Shadow DOM, so an embedded page can use them directly:

```css
:host {
  display: block;
  font-family: var(--pico-font-family);
  color: var(--pico-color-text-primary);
  background: var(--pico-color-bg-light);
}

.panel {
  border-radius: var(--pico-radius-default);
  box-shadow: var(--pico-shadow-elevation-1);
}
```

For a standalone preview, import the stylesheet in the document entry point:

```ts
import "@picodata/ui-kit/tokens.css";
```

It includes Montserrat's `@font-face` rules and references font files shipped
with the package. An embedded plugin can inherit the host's tokens and use its
loaded font without bundling this stylesheet again. Font sources live in
`webui/src/assets/fonts/montserrat`; the host build emits one copy of each font,
shared by the stylesheet and HTML preload links.

For a programmatic theme configuration, use the typed constants:

```ts
import { tokens } from "@picodata/ui-kit/tokens";

const primaryColor = tokens.palette.primary.main;
```

The same values are available through `@picodata/ui-kit/tokens.json` and
`@picodata/ui-kit/tokens.ts`. Map these values or CSS variables in the plugin's
own Tailwind or MUI configuration; the package contains no UI components.

| Theme key                      | CSS property                     |
| ------------------------------ | -------------------------------- |
| `palette.<group>.<key>`        | `--pico-palette-<group>-<key>`   |
| `common.colors.<group>.<key>`  | `--pico-<key>`                   |
| `common.variables.elevation1`  | `--pico-shadow-elevation-1`      |
| `common.variables.elevationBg` | `--pico-shadow-elevation-bg`     |
| Other `common.variables.<key>` | `--pico-<key>`                   |
| `common.zIndex.<key>`          | `--pico-<key>`                   |
| `shape.borderRadius`           | `--pico-radius-default` (pixels) |

Keys are converted to kebab-case, with a leading `$` removed.
`--pico-color-text-primary` and `--pico-color-text-secondary` are aliases for
the corresponding palette text colors.

## Development

From the `webui` directory, install workspace dependencies and run the checks:

```bash
yarn install --immutable
yarn test:ui-kit
```

The host uses the local workspace package, so development does not require
publishing it to the registry.

`src/shared/theme/theme.ts` remains the source of truth. Its `themeOptions`
and `fontFaces` declarations contain standalone values; the generator reads
them without loading the host's React or MUI runtime. Generated files live in
`packages/ui-kit/dist` and are not committed. Consumers install ready-to-use
files and do not run the generator.

Vite generates the package before starting or building the host and regenerates
it when the theme or font files change during development. To generate it
separately, run `yarn build:ui-kit`. Publication always regenerates the package
before packing it. It stages the package in a temporary directory and includes
`AUTHORS` and `LICENSE` from the repository root unchanged; the package has no
separate copies of these files in Git.

Packaging and publication use the Yarn release pinned in `webui/package.json`
and bundled in `.yarn/releases`. `yarn pack` creates the archive used for the
version check; Node.js calculates its SHA-512 integrity and SHA-1 checksum.
`yarn npm publish` packs and publishes the same staged directory. Both operations
use the same files and Yarn release.

## Versioning

The package has its own SemVer version in `package.json`, independent of the
Picodata release. The host uses this version as `webuiSdk.version`. Bump it when
published contents other than the root `AUTHORS` file change; breaking changes
before `1.0.0` require a minor bump.
SDK types and design tokens share this version. A theme change must therefore
include a package version bump before the next Picodata release.

On stable Picodata release tags (`X.Y.Z`), CI publishes the package to the
Picodata Nexus npm registry. Configure these GitLab CI/CD variables:

- `NPM_REGISTRY_URL`: the writable Nexus npm repository URL. Keep it
  unprotected so merge request pipelines can check the published version.
- `NPM_REGISTRY_TOKEN`: the npm Bearer token with read and publish access.
  Store it as a masked, protected variable available to release tag pipelines.

The registry must support npm Bearer authentication. The temporary Yarn
configuration scopes credentials to this registry and references the token
through the environment. No credentials are stored in the package.
Release packaging waits for publication. An identical published version is
skipped. If the archive hashes differ, the published archive is downloaded from
the configured registry and its files are compared with the local archive.
Only the root `AUTHORS` file is excluded from this comparison. An AUTHORS-only
change skips publication and leaves the published archive intact; each new
version includes the current root `AUTHORS`. Changes to `LICENSE` or any other
file under the same version, and registry errors, fail the job.

Merge request pipelines that touch `webui` or `LICENSE` run the same
comparison without publishing, so a missing version bump fails the merge request
instead of the release. The check reads the registry anonymously and does not
need `NPM_REGISTRY_TOKEN`. Enable anonymous read access to package metadata and
archives in Nexus for these checks. To run it locally:

```bash
NPM_REGISTRY_URL=<registry URL> yarn check:ui-kit
```

Release CI runs `yarn publish:ui-kit` with both registry variables available.

## Resources

- [SDK types](index.d.ts)
- [Plugin WebUI ADR](https://git.picodata.io/core/picodata/-/blob/master/doc/adr/2026-09-09-plugin-ui.md)

## License

This project is licensed under the BSD-2-Clause license. See the repository's
[LICENSE](https://git.picodata.io/core/picodata/-/blob/master/LICENSE) for details.
The published package includes this file and the root `AUTHORS` file.
