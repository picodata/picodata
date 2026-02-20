import { styled } from "@mui/material";

export const Root = styled("div")({
  width: "40px",
  height: "16px",
  background: "#fdfdfd",
  borderRadius: "10px",
  padding: "2px",
  boxSizing: "border-box",
});

export const Circle = styled("div")<{ $checked: boolean }>(({ $checked }) => ({
  width: "12px",
  height: "12px",
  borderRadius: "50%",
  backgroundColor: $checked ? "#c0ecd4" : "#848484",
  marginLeft: $checked ? "auto" : "none",
}));
