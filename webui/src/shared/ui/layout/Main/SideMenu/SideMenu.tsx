import { useEffect, useRef, useState } from "react";
import { z } from "zod";

import { BurgerIcon } from "shared/icons/BurgerIcon";
import { useOutsideClickEvent } from "shared/react/hooks/useOutsideClickEvent";
import { InstancesIcon } from "shared/icons/navLinks/InstancesIcon";
import { Routes } from "shared/router/config";
import { useLsState } from "shared/localStorage/hooks/useLsState";
import { useTranslation } from "shared/intl";
import { usePluginUiPages } from "shared/entity/pluginUi";
import { groupPluginUiPages } from "shared/entity/pluginUi/utils";
import { useSessionStore } from "shared/session";
import { useAppConfig } from "shared/entity/config";

import {
  MenuIcon,
  MenuNavLink,
  NavLinksList,
  NavLinkText,
  Root,
} from "./StyledComponents";
import { PluginNavGroup } from "./PluginNavGroup";

export const SideMenu = () => {
  const [isOpen, setIsOpen] = useLsState({
    key: "sideMenuOpenState",
    schema: z.boolean(),
    defaultValue: false,
  });

  const containerRef = useRef<HTMLDivElement>(null);
  const [isExpanded, setIsExpanded] = useState(isOpen);

  useEffect(() => {
    if (!isOpen) setIsExpanded(false);
  }, [isOpen]);

  useOutsideClickEvent(containerRef, () => {
    setIsOpen(false);
  });

  const { translation } = useTranslation();
  const sideMenuTranslations = translation.sideMenu;
  const { data: pages = [] } = usePluginUiPages();
  const [tokens] = useSessionStore();
  const { isAuthEnabled } = useAppConfig();

  const navLinks = [
    {
      to: Routes.NODES,
      label: sideMenuTranslations.navLinks.instances.label,
      icon: <InstancesIcon style={{ flexShrink: 0 }} />,
    },
  ];

  return (
    <Root
      ref={containerRef}
      $isOpen={isOpen}
      onTransitionEnd={(event) => {
        if (
          event.target === event.currentTarget &&
          event.propertyName === "width" &&
          isOpen
        ) {
          setIsExpanded(true);
        }
      }}
    >
      <MenuIcon $isOpen={isOpen} onClick={() => setIsOpen(!isOpen)}>
        <BurgerIcon />
      </MenuIcon>
      <NavLinksList $isOpen={isOpen && isExpanded}>
        {(!isAuthEnabled || tokens.auth) &&
          navLinks.map((link) => {
            return (
              <MenuNavLink key={link.to} to={link.to}>
                {link.icon}
                <NavLinkText>{link.label}</NavLinkText>
              </MenuNavLink>
            );
          })}
        {groupPluginUiPages(pages).map(([plugin, pluginPages]) => (
          <PluginNavGroup key={plugin} plugin={plugin} pages={pluginPages} />
        ))}
      </NavLinksList>
      {/* Пример смены языка
      <div
        onClick={() =>
          setLocale(locale === IntlLocale.EN ? IntlLocale.RU : IntlLocale.EN)
        }
      >
        Change Locale
      </div> */}
    </Root>
  );
};
