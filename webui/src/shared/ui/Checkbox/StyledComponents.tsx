import { styled } from "@mui/material";
import { CSSProperties } from "react";

export const Root = styled("div")({
  position: "relative",
  display: "inline-flex",
  textDecoration: "none",
  alignItems: "center",
  justifyContent: "center",
  width: "fit-content",
  flex: "0 0 auto",
  touchAction: "none",
  cursor: "pointer",
  border: "none",
  outline: "none",
  appearance: "none",
  "-webkit-tap-highlight-color": "transparent",
  gap: "16px",
  "&:disabled ": {
    cursor: "initial",
  },
});

export const StyleInput = styled("input")(({ theme }) => ({
  width: "24px",
  height: "24px",
  outline: "none !important",
  appearance: "none",
  border: `1px solid ${theme.common.colors.primary.colorPrimary}`,
  borderRadius: "4px",
  transition: "border-width 100ms ease",
  "&:focus ": {
    borderWidth: "2px",
  },
  "&:hover, &:focus": {
    borderColor: theme.common.colors.primary.colorPrimaryHover,
  },
  "&:active ": {
    borderColor: theme.common.colors.primary.colorPrimarySelected,
  },

  "&:disabled": {
    borderColor: theme.common.colors.disabled.colorDisabled,
  },

  "&:checked": {
    backgroundColor: theme.common.colors.primary.colorPrimary,
  },

  "&:checked:hover, &:checked:focus ": {
    backgroundColor: theme.common.colors.primary.colorPrimaryHover,
  },

  "&:checked:active": {
    backgroundColor: theme.common.colors.primary.colorPrimarySelected,
  },

  "&:checked:disabled": {
    backgroundColor: theme.common.colors.disabled.colorDisabled,
  },

  "&:checked + .check ": {
    stroke: theme.common.colors.bg.colorBgWhite,
  },

  "&:checked:disabled + .check": {
    stroke: theme.common.colors.disabled.colorIconDisabled,
  },
}));

export const checkSx: CSSProperties = {
  position: "absolute",
  left: "8px",
  top: "7px",
  display: "block",
  width: "16px",
  height: "16px",
  stroke: "#F7F4F1",
  pointerEvents: "none",
};

export const Content = styled("label")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextBlack,
  userSelect: "none",
}));
