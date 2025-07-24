import React from "react";
import cn from "classnames";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";
import { LinkBreakSystemIcon } from "shared/icons/LinkBreakSystemIcon";

import styles from "./Clippable.module.scss";

export interface ClippableIconProps {
  className?: string;
  isClipping?: boolean;
  onClick?: VoidFunction;
}

export const ClippableIcon: React.FC<ClippableIconProps> = (props) => {
  const { onClick, className, isClipping } = props;

  return (
    <div
      className={cn(
        styles.clipIcon,
        isClipping ? styles.copied : "",
        className
      )}
      onClick={onClick}
    >
      <LinkSystemIcon
        width={16}
        height={16}
        className={cn(styles.icon, styles.iconCopy)}
      />
      <LinkBreakSystemIcon
        width={16}
        height={16}
        className={cn(styles.icon, styles.iconCopied)}
      />
    </div>
  );
};
