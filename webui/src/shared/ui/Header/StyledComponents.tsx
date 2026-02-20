import { styled } from "@mui/material";

export const StyledHeader = styled("header")(({ theme }) => ({
  width: "100%",
  height: "48px",
  background: theme.common.colors.bg.colorBgGray,
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  flexShrink: 0,
  paddingLeft: "40px",
}));

export const Actions = styled("div")({
  padding: "0 16px",
});
