import { PluginUiPageType } from "shared/entity/pluginUi";

export type PluginPageErrorStage =
  | "entry"
  | "import"
  | "registration"
  | "mount"
  | "sdk"
  | "render";

export class PluginPageLoadError extends Error {
  constructor(
    public readonly stage: PluginPageErrorStage,
    public readonly originalError: unknown
  ) {
    super(`Plugin page failed at ${stage}`);
    this.name = "PluginPageLoadError";
  }
}

export function logPluginPageError(
  {
    plugin,
    slug,
    version,
  }: Pick<PluginUiPageType, "plugin" | "slug" | "version">,
  stage: PluginPageErrorStage,
  error: unknown,
  componentStack?: string
) {
  console.error(
    "[PluginPage]",
    { plugin, slug, version, stage, componentStack },
    error
  );
}
