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
    default:
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
}>(({ $size, theme }) => {
  let fontSize: string;
  if ($size === "small") fontSize = "16px";
  else if ($size === "extraSmall") fontSize = "14px";
  else fontSize = "20px";

  return {
    display: "flex",
    alignItems: "center",
    fontSize,
    fontWeight: 400,
    lineHeight: "20px",
    color: theme.common.colors.typography.colorTextBlack,
  };
});

export const Icon = styled("span")<{
  $type: "left" | "right";
  $size: Required<ButtonProps>["size"];
}>(({ $size, $type }) => {
  const iconSize = $size === "large" ? "24px" : "16px";
  let iconMargin: string;
  if ($size === "normal") {
    iconMargin = "none";
  } else if ($size === "extraSmall") {
    iconMargin = "6px";
  } else {
    iconMargin = "8px";
  }

  return {
    height: iconSize,
    width: iconSize,
    marginRight: $type === "left" ? iconMargin : "none",
    marginLeft: $type === "right" ? iconMargin : "none",
    "& svg": {
      height: "100%",
      width: "100%",
    },
  };
});
