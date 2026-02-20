import { styled } from "@mui/material";

export const Root = styled("div")<{ $upDirection: boolean }>(
  ({ $upDirection, theme }) => ({
    display: "flex",
    flexDirection: "column",
    justifyContent: "flex-start",
    maxHeight: "350px",
    overflow: "auto",
    marginTop: $upDirection ? 0 : theme.common.variables.dropdownOffset,
    position: "absolute",
    top: $upDirection ? "auto" : "100%",
    left: 0,
    width: "100%",
    boxSizing: "border-box",
    borderRadius: "4px",
    background: theme.common.colors.bg.colorBgWhite,
    boxShadow: theme.common.variables.elevation1,
    zIndex: theme.common.zIndex.zIndexDropdown,
    ...($upDirection
      ? {
          bottom: "100%",
          marginBottom: theme.common.variables.dropdownOffset,
        }
      : {}),
  })
);
