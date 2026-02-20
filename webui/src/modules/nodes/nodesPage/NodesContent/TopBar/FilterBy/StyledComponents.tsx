import { styled } from "@mui/material";
import { SxProps } from "@mui/system/styleFunctionSx";

export const TitleWrapper = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  marginBottom: "16px",
});

export const TitleText = styled("span")({
  fontSize: "24px",
  fontWeight: 500,
  display: "flex",
  alignItems: "center",
});

export const Close = styled("span")({
  cursor: "pointer",
  width: "32px",
  height: "32px",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
});

export const bodySx: SxProps = {
  width: "365px",
  padding: "40px 24px",
  minHeight: "400px",
  height: "auto",
  maxHeight: "800px",
};
