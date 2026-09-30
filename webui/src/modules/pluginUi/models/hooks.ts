import { useEffect, useRef, useState } from "react";
import { useTheme } from "@mui/material";

import { PluginUiPageType } from "shared/entity/pluginUi";
import { useTranslation } from "shared/intl";
import { WebuiPageElement } from "shared/webuiSdk";
import { createWebuiSdk } from "shared/webuiSdk/runtime";

import {
  loadPluginPage,
  logPluginPageError,
  PluginPageLoadError,
} from "../utils";

export function usePluginPage(page: PluginUiPageType) {
  const container = useRef<HTMLDivElement>(null);
  const runtime = useRef<ReturnType<typeof createWebuiSdk>>();
  const [failed, setFailed] = useState(false);
  const [loading, setLoading] = useState(true);
  const { locale } = useTranslation();
  const theme = useTheme().palette.mode;
  const context = useRef({ theme, locale });
  context.current = { theme, locale };

  const { plugin, slug, version, entry, title, auth } = page;
  useEffect(() => {
    if (failed) return;
    let active = true;
    let reported = false;
    const fail = (error: unknown, stage: "sdk" | "mount") => {
      if (!active || reported) return;
      reported = true;
      logPluginPageError(
        { plugin, slug, version },
        error instanceof PluginPageLoadError ? error.stage : stage,
        error instanceof PluginPageLoadError ? error.originalError : error
      );
      setFailed(true);
    };
    let element: WebuiPageElement | undefined;
    const sdk = createWebuiSdk(
      context.current.theme,
      context.current.locale,
      (error) => fail(error, "sdk")
    );
    runtime.current = sdk;
    loadPluginPage({ plugin, slug, version, entry, title, auth })
      .then((tag) => {
        if (!active) return;
        element = document.createElement(tag) as WebuiPageElement;
        const Page = customElements.get(tag);
        if (!Page || !(element instanceof Page)) {
          throw new Error("Plugin element construction failed");
        }
        element.webuiSdk = sdk.sdk;
        container.current?.append(element);
        setLoading(false);
      })
      .catch((error: unknown) => fail(error, "mount"));
    return () => {
      active = false;
      sdk.dispose();
      element?.remove();
      runtime.current = undefined;
    };
  }, [plugin, slug, version, entry, title, auth, failed]);

  useEffect(() => {
    runtime.current?.update(theme, locale);
  }, [theme, locale]);
  return { container, failed, loading };
}
