import { useRef } from "react";
import { SxProps } from "@mui/material";

import { Content, Icon, Root } from "./StyledComponents";

export type ButtonProps = {
  children?: React.ReactNode;
  leftIcon?: React.ReactNode;
  rightIcon?: React.ReactNode;
  theme?: "primary" | "secondary";
  size?: "large" | "normal" | "small" | "extraSmall";
  disabled?: boolean;
  sx?: SxProps;
} & React.ButtonHTMLAttributes<HTMLButtonElement>;

export const Button = (props: ButtonProps) => {
  const {
    children,
    size = "normal",
    theme = "primary",
    disabled = false,
    leftIcon,
    rightIcon,
    sx = {},
    ...buttonProps
  } = props;
  const ref = useRef<HTMLButtonElement>(null);

  return (
    <Root
      sx={sx}
      ref={ref}
      {...buttonProps}
      onClick={(e) => {
        if (buttonProps.onClick) {
          buttonProps.onClick(e);
        }
        ref.current?.focus();
      }}
      disabled={disabled}
      $size={size}
      $theme={theme}
      $disabled={disabled}
    >
      <Content $size={size}>
        {leftIcon && (
          <Icon $type={"left"} $size={size}>
            {leftIcon}
          </Icon>
        )}
        {children}
        {rightIcon && (
          <Icon $type={"right"} $size={size}>
            {rightIcon}
          </Icon>
        )}
      </Content>
    </Root>
  );
};
