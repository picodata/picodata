import { styled } from "@mui/material";
export const Root = styled("div")({
  width: "100vw",
  minWidth: "1440px",
  display: "flex",
  flexDirection: "column",
});
export const LayoutMain = styled("main")({
  display: "flex",
  flexGrow: 1,
  overflow: "hidden",
  position: "relative",
  padding: "0 140px",
  minWidth: "fit-content",
});

export const BodyWrapper = styled("div")({
  display: "flex",
  flexGrow: 1,
});
