import { styled } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextGrey,
  fontSize: "14px",
  fontWeight: 400,
  lineHeight: "16px",
}));
