import { styled, SxProps } from "@mui/material";

export const gridWrapperSx: SxProps = {
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
};

export const topBarSx: SxProps = {
  margin: "16px 24px",
};

export const List = styled("div")({
  display: "flex",
  flexDirection: "column",
  gap: "16px",
  flexGrow: 1,
  padding: "0 16px",
  margin: "0 8px",
  paddingBottom: "40px",
});
