import { styled } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  position: "relative",
  backgroundColor: theme.common.colors.bg.colorBgTabs,
  padding: "4px 24px",
  boxShadow: "1px 1px 3px 0px #9d99ac",
  borderRadius: "4px",
  fontSize: "12px",
  lineHeight: "16px",
  fontWeight: 400,
}));
