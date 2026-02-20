import { styled } from "@mui/material";
export const Root = styled("div")({
  height: "100%",
  position: "relative",
});
export const WorkSpace = styled("div")({
  overflow: "hidden",
  width: "100vw",
  minWidth: "1440px",
  display: "grid",
  gridTemplateRows: "min-content 1fr",
  height: "100%",
});
export const LayoutMain = styled("main")({
  overflow: "hidden",
  margin: "0 130px 28px 130px",
});
