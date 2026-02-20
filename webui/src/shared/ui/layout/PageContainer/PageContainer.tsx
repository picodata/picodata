import { Box, styled } from "@mui/material";

export const PageContainer = styled(Box)({
  paddingTop: "20px",
  display: "grid",
  gridTemplateRows: "min-content min-content 1fr",
  height: "100%",
  overflow: "hidden",
});
