import React from "react";
import cn from "classnames";

import styles from "./Button.module.scss";

export type ButtonProps = {
  children: React.ReactNode;
  leftIcon?: React.ReactNode;
  rightIcon?: React.ReactNode;
  theme?: "primary" | "secondary";
  size?: "large" | "normal" | "small" | "extraSmall";
  disabled?: boolean;
} & React.ButtonHTMLAttributes<HTMLButtonElement>;

export const Button: React.FC<ButtonProps> = (props) => {
  const {
    children,
    size = "normal",
    theme = "primary",
    disabled = false,
    leftIcon,
    rightIcon,
    ...buttonProps
  } = props;

  return (
    <button
      {...buttonProps}
      className={cn(
        styles.container,
        styles[size],
        styles[theme],
        buttonProps.className
      )}
      disabled={disabled}
    >
      <span className={styles.content}>
        {leftIcon && <span className={styles.leftIcon}>{leftIcon}</span>}
        {children}
        {rightIcon && <span className={styles.rightIcon}>{rightIcon}</span>}
      </span>
    </button>
  );
};
