import { styled } from "@mui/material";

import { CloseIcon as SharedCloseIcon } from "shared/icons/CloseIcon";

export const Root = styled("div")(({ theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgWhite,
  borderRadius: "4px",
  padding: "40px 24px",
  boxShadow: theme.common.variables.elevationBg,
}));
export const TitleWrapper = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  marginBottom: "16px",
});
export const TitleText = styled("span")({
  fontSize: "24px",
  fontWeight: "600",
  alignItems: "center",
  gap: "8px",
  justifyContent: "center",
  display: "flex",
});
export const CloseIcon = styled(SharedCloseIcon)({
  cursor: "pointer",
});
