import { styled } from "@mui/material";
import { SxProps } from "@mui/system/styleFunctionSx";

export const Root = styled("div")({
  display: "flex",
  flexDirection: "column",
});

export const Controls = styled("div")({
  display: "flex",
  justifyContent: "space-between",
});

export const Right = styled("div")({
  display: "flex",
  gap: "24px",
});
export const filtersSx: SxProps = {
  marginTop: "8px",
};
