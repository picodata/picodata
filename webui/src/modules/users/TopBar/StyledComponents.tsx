import { styled } from "@mui/material";
import { SxProps } from "@mui/system/styleFunctionSx";
import { CSSProperties } from "react";

export const Root = styled("div")({
  display: "flex",
  flexDirection: "column",
});

export const Controls = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
});

export const RightContainer = styled("div")({
  display: "flex",
  gap: "24px",
});

export const inputContainerSx: SxProps = {
  width: "460px",
};

export const searchIconSx: CSSProperties = {
  color: "#050505",
};
