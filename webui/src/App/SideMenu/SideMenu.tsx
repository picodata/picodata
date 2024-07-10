import { useRef } from "react";
import cn from "classnames";
import { NavLink } from "react-router-dom";
import { z } from "zod";

import { BurgerIcon } from "shared/icons/BurgerIcon";
import { useOutsideClickEvent } from "shared/react/hooks/useOutsideClickEvent";
import { InstancesIcon } from "shared/icons/navLinks/InstancesIcon";
import { URL_CONFIG } from "shared/router/config";
import { useLsState } from "shared/localStorage/hooks/useLsState";
import { useTranslation } from "shared/intl";
import { UsersIcon } from "shared/icons/navLinks/UsersIcon";

import styles from "./SideMenu.module.scss";

export const SideMenu = () => {
  const [isOpen, setIsOpen] = useLsState({
    key: "sideMenuOpenState",
    schema: z.boolean(),
    defaultValue: false,
  });

  const containerRef = useRef<HTMLDivElement>(null);

  useOutsideClickEvent(containerRef, () => {
    setIsOpen(false);
  });

  const { translation } = useTranslation();
  const sideMenuTranslations = translation.sideMenu;

  const navLinks = [
    {
      to: URL_CONFIG.NODES.absolutePath,
      label: sideMenuTranslations.navLinks.instances.label,
      icon: <InstancesIcon className={styles.icon} />,
    },
    {
      to: URL_CONFIG.USERS.absolutePath,
      label: sideMenuTranslations.navLinks.users.label,
      icon: <UsersIcon className={styles.icon} />,
    },
  ];

  return (
    <div
      className={cn(styles.container, isOpen && styles.openContainer)}
      ref={containerRef}
    >
      <div
        className={cn(styles.menuIcon, isOpen && styles.openMenuIcon)}
        onClick={() => setIsOpen(!isOpen)}
      >
        <BurgerIcon />
      </div>
      <div className={styles.navLinksList}>
        {navLinks.map((link) => {
          return (
            <NavLink
              key={link.to}
              to={link.to}
              className={({ isActive }) => {
                if (isActive) return cn(styles.navLink, styles.activeNavLink);

                return styles.navLink;
              }}
            >
              {link.icon}
              <span className={styles.navLinkText}>{link.label}</span>
            </NavLink>
          );
        })}
      </div>
      {/* Пример смены языка
      <div
        onClick={() =>
          setLocale(locale === IntlLocale.EN ? IntlLocale.RU : IntlLocale.EN)
        }
      >
        Change Locale
      </div> */}
    </div>
  );
};
