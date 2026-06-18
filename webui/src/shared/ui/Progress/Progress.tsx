import { Box, CircularProgress, styled } from "@mui/material";

export const Root = styled(Box)({
  overflow: "hidden",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  height: "100%",
});

export const Progress = () => (
  <Root>
    <CircularProgress />
  </Root>
);
