import { useState } from "react";
import { z } from "zod";

import { PluginUiPageType } from "shared/entity/pluginUi";
import {
  getPluginUiAssetBase,
  getPluginUiPagePath,
} from "shared/entity/pluginUi/utils";
import { usePluginUiTranslations } from "shared/entity/pluginUi/translations";
import { useLsState } from "shared/localStorage/hooks/useLsState";

import {
  MenuNavLink,
  NavLinkText,
  PluginGroup,
  PluginSummary,
  PluginPages,
  PluginPageIcon,
  PluginPageIconSlot,
} from "./StyledComponents";

function PageIcon({ src }: { src: string }) {
  const [failed, setFailed] = useState(false);
  return (
    <PluginPageIconSlot aria-hidden="true">
      {!failed && (
        <PluginPageIcon src={src} alt="" onError={() => setFailed(true)} />
      )}
    </PluginPageIconSlot>
  );
}

export function PluginNavGroup({
  plugin,
  pages,
}: {
  plugin: string;
  pages: PluginUiPageType[];
}) {
  const [isOpen, setIsOpen] = useLsState({
    key: `pluginNavGroupOpen:${plugin}`,
    schema: z.boolean(),
    defaultValue: false,
  });
  const title = usePluginUiTranslations(
    plugin,
    pages[0].version,
    pages.map((page) => page.title)
  );
  return (
    <PluginGroup
      open={isOpen}
      onToggle={(event) => setIsOpen(event.currentTarget.open)}
    >
      <PluginSummary>
        <NavLinkText>{plugin}</NavLinkText>
      </PluginSummary>
      <PluginPages>
        {pages.map((page) => {
          const icon = `${getPluginUiAssetBase(
            page
          )}/icons/${encodeURIComponent(page.slug)}.svg`;
          return (
            <MenuNavLink key={page.slug} to={getPluginUiPagePath(page)}>
              <PageIcon key={icon} src={icon} />
              <NavLinkText>{title(page.title)}</NavLinkText>
            </MenuNavLink>
          );
        })}
      </PluginPages>
    </PluginGroup>
  );
}
