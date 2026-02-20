import { ReactNode } from "react";

export type ButtonProps = {
  children?: ReactNode;
  leftIcon?: ReactNode;
  rightIcon?: ReactNode;
  theme?: "primary" | "secondary";
  size?: "large" | "normal" | "small" | "extraSmall";
  disabled?: boolean;
};
