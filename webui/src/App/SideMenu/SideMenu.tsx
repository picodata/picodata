import { useRef } from "react";
import cn from "classnames";
import { NavLink } from "react-router-dom";
import { z } from "zod";

import { BurgerIcon } from "shared/icons/BurgerIcon";
import { useOutsideClickEvent } from "shared/react/hooks/useOutsideClickEvent";
import { NodesIcon } from "shared/icons/navLinks/NodesIcon";
import { URL_CONFIG } from "shared/router/config";
import { useLsState } from "shared/localStorage/hooks/useLsState";
import { useTranslation } from "shared/intl";

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

  return (
    <div
      className={cn(styles.container, isOpen && styles.openContainer)}
      ref={containerRef}
    >
      <div className={styles.menuIcon} onClick={() => setIsOpen(!isOpen)}>
        <BurgerIcon />
      </div>
      <div className={styles.navLinksList}>
        <NavLink
          to={URL_CONFIG.NODES.absolutePath}
          className={(isActive) => {
            if (isActive) return cn(styles.navLink, styles.activeNavLink);

            return styles.navLink;
          }}
        >
          <NodesIcon />
          <span className={styles.navLinkText}>
            {sideMenuTranslations.navLinks.instances.label}
          </span>
        </NavLink>
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
