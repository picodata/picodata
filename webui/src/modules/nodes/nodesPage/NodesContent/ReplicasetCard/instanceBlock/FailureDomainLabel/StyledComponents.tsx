import { styled } from "@mui/material";
import { SxProps } from "@mui/system/styleFunctionSx";

export const containerSx: SxProps = {
  display: "inline-flex",
  columnGap: "4px",
  alignItems: "center",
};

export const Text = styled("span")({
  flexShrink: 0,
});
