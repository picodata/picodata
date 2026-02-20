import { styled } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  fontSize: "24px",
  fontWeight: 400,
  lineHeight: "30px",
  color: theme.common.colors.typography.colorTextOrange,
  margin: "8px 10px 14px 10px",
}));
export const Prefix = styled("span")({
  userSelect: "none",
});
