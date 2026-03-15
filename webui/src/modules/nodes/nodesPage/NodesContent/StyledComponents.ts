import { Box, styled } from "@mui/material";

import { NoData } from "../../../../shared/ui/NoData/NoData";

export const Root = styled(Box)({
  padding: "14px 0",
  overflow: "visible",
  display: "grid",
  gridTemplateRows: "min-content 1fr",
  minHeight: "100%",
});

export const ToolBarContainer = styled(Box)(({ theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgWhite,
  borderRadius: "16px 16px 0 0",
  padding: 10,
}));

export const ContentContainer = styled(Box)({
  overflow: "hidden",
  borderRadius: "0 0 16px 16px",
});
export const NodesNoData = styled(NoData)(({ theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgWhite,
  margin: 0,
  borderRadius: "0 0 16px 16px",
  paddingBottom: 16,
}));
