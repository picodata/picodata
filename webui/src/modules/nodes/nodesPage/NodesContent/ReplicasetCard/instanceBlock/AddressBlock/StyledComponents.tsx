import { styled } from "@mui/material";
import { SxProps } from "@mui/system/styleFunctionSx";

export const Root = styled("div")({
  flex: 1,
  display: "grid",
  alignItems: "start",
  gridTemplateColumns: "auto auto",
  columnGap: "4px",
  rowGap: "2px",
  overflow: "hidden",
  padding: "4px max(8px, 2%)",
  maxWidth: "max-content",
  minWidth: "150px",
});
export const AddressLabel = styled("span")({
  fontSize: "12px",
  lineHeight: "16px",
  textAlign: "center",
});

export const addressValueSx: SxProps = {
  display: "flex",
  alignItems: "center",
  overflow: "hidden",
  "& *": {
    fontSize: "14px",
    lineHeight: "16px",
    overflow: "hidden",
  },
};
