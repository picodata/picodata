import { styled } from "@mui/material";

export const KeyValueField = styled("div")({
  marginTop: "12px",
  display: "flex",
});

export const KeyValueFieldCenter = styled(KeyValueField)({
  alignItems: "center",
});

export const Container = styled("div")({
  "&:nth-child(2)": {
    marginTop: "24px",
  },
});
export const DeleteContainer = styled("div")({
  display: "flex",
  alignItems: "center",
});
export const DeleteElement = styled("div")({
  display: "inline-flex",
  color: "#dd4a4a",
  cursor: "pointer",
  padding: "4px",
  marginLeft: "auto",
});
