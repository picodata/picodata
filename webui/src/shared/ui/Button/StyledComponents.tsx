import { styled } from "@mui/material";

import { ButtonProps } from "./Spinner.types";

export const Root = styled("button", {
  name: "Button",
  slot: "root",
})<{
  $disabled: boolean;
  $size: Required<ButtonProps>["size"];
  $theme: Required<ButtonProps>["theme"];
}>(({ $disabled, $size, $theme, theme }) => {
  let padding: string;
  switch ($size) {
    default:
    case "large":
      padding = "16px 24px";
      break;
    case "normal": {
      padding = "8px 24px";
      break;
    }
    case "small": {
      padding = "8px 20px";
      break;
    }
    case "extraSmall": {
      padding = "6px 16px";
      break;
    }
  }
  return {
    display: "inline-flex",
    textDecoration: "none",
    alignItems: "center",
    justifyContent: "center",
    width: "fit-content",
    flex: "0 0 auto",
    touchAction: "none",
    cursor: $disabled ? "initial" : "pointer",
    border: "none",
    outline: "none",
    appearance: "none",
    "&:-webkit-tap-highlight-color": "transparent",
    borderRadius: "4px",
    padding,
    backgroundColor:
      $theme === "primary"
        ? theme.common.colors.primary.colorPrimary
        : theme.common.colors.secondary.colorSecondary,
    "&:hover, &:focus": {
      backgroundColor:
        $theme === "primary"
          ? theme.common.colors.primary.colorPrimaryHover
          : theme.common.colors.secondary.colorSecondaryHover,
    },

    "&:active": {
      backgroundColor:
        $theme === "primary"
          ? theme.common.colors.primary.colorPrimarySelected
          : theme.common.colors.secondary.colorSecondarySelected,
    },

    "&:disabled": {
      backgroundColor:
        $theme === "primary"
          ? theme.common.colors.primary.colorPrimaryDisabled
          : theme.common.colors.secondary.colorSecondaryDisabled,
    },
  };
});

export const Content = styled("span", {
  name: "Content",
  slot: "content",
})<{
  $size: Required<ButtonProps>["size"];
}>(({ $size, theme }) => ({
  display: "flex",
  alignItems: "center",
  fontSize:
    $size === "small" ? "16px" : $size === "extraSmall" ? "14px" : "20px",
  fontWeight: 400,
  lineHeight: "20px",
  color: theme.common.colors.typography.colorTextBlack,
}));

export const Icon = styled("span")<{
  $type: "left" | "right";
  $size: Required<ButtonProps>["size"];
}>(({ $size, $type }) => ({
  height: $size === "large" ? "24px" : "16px",
  width: $size === "large" ? "24px" : "16px",
  marginRight:
    $type === "left" && $size !== "normal"
      ? $size === "extraSmall"
        ? "6px"
        : "8px"
      : "none",
  marginLeft:
    $type === "right" && $size !== "normal"
      ? $size === "extraSmall"
        ? "6px"
        : "8px"
      : "none",
  "& svg": {
    height: "100%",
    width: "100%",
  },
}));
