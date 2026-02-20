import { styled, SxProps } from "@mui/material";

export const buttonApplySx: SxProps = {
  flexGrow: 1,
};
export const buttonClearSx: SxProps = {
  flexShrink: 0,
  marginLeft: "16px",
};

export const Field = styled("div")({
  marginBottom: "24px",
});

export const Label = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextBlack,
  fontSize: "16px",
  fontWeight: 400,
  lineHeight: "24px",
  display: "flex",
  alignItems: "center",
  columnGap: "4px",
}));

export const Scroll = styled("div")({
  overflow: "auto",
  maxHeight: "492px",
  marginTop: "4px",
});

export const Footer = styled("div")({
  marginTop: "56px",
  display: "flex",
});
export const AddFilter = styled("div")({
  width: "32px",
  height: "32px",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  background: "#e8ebf1",
  borderRadius: "4px",
  cursor: "pointer",
});
