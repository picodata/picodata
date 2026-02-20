import { styled } from "@mui/material";

export const Paper = styled("div")(({ theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgWhite,
  borderRadius: "16px",
  boxShadow: theme.common.variables.elevationBg,
}));
