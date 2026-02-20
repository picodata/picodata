import { Box, styled, SxProps } from "@mui/material";

export const clusterInfoSx: SxProps = {
  marginBottom: "24px",
};

export const NodesPageContainer = styled(Box)({
  gridTemplateRows: "min-content 1fr",
  paddingTop: "20px",
  display: "grid",
  overflow: "hidden",
  height: "100%",
});
export const ContentContainer = styled(Box)({
  overflow: "hidden",
});
export const LoadContainer = styled(Box)({
  overflow: "hidden",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  height: "100%",
});
