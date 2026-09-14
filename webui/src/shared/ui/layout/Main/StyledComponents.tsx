import { styled } from "@mui/material";
export const Root = styled("div")({
  height: "100%",
  position: "relative",
});
export const WorkSpace = styled("div")({
  overflow: "hidden",
  width: "100vw",
  minWidth: "1024px",
  display: "grid",
  gridTemplateRows: "min-content 1fr",
  height: "100%",
});
export const LayoutMain = styled("main")({
  overflowX: "auto",
  overflowY: "hidden",
  margin: "0 clamp(24px, 6vw, 130px) 28px clamp(24px, 6vw, 130px)",
});
