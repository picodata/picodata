import { styled } from "@mui/material";
import { CSSProperties } from "react";

export const Root = styled("div")<{ $inline: boolean }>(({ $inline }) =>
  $inline
    ? {
        display: "flex",
        flexWrap: "nowrap",
      }
    : {}
);

export const ClipIcon = styled("div")<{ $isClipping: boolean }>(
  ({ $isClipping }) => ({
    display: "flex",
    flexDirection: "column",
    width: "16px",
    height: "16px",
    overflow: "hidden",
    cursor: $isClipping ? "auto" : "pointer",
  })
);

export const getIconCopySx = (isClipping: boolean): CSSProperties => ({
  position: "absolute",
  pointerEvents: "none",
  transition: "transform 200ms, opacity 200ms",
  opacity: isClipping ? 0 : 1,
  transform: isClipping ? "translate3d(0, 100%, 0)" : "translate3d(0, 0, 0)",
});

export const getIconCopiedSx = (isClipping: boolean): CSSProperties => ({
  opacity: isClipping ? 1 : 0,
  transform: isClipping ? "translate3d(0, 0, 0)" : "translate3d(0, -100%, 0)",
});
