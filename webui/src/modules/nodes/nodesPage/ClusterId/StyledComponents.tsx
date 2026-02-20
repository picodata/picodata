import { styled } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  fontSize: "24px",
  fontWeight: 400,
  lineNeight: "30px",
  marginBottom: "16px",
  color: theme.common.colors.typography.colorTextOrange,
}));
export const Prefix = styled("span")({
  userSelect: "none",
});
