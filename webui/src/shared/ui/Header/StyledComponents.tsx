import { styled } from "@mui/material";
export const StyledHeader = styled("header")(({ theme }) => ({
  width: "100%",
  height: "48px",
  background: theme.common.colors.bg.colorBgGray,
  display: "grid",
  gridTemplateColumns: "min-content 1fr min-content",
  paddingLeft: "10px",
  gap: 20,
  "& > *": {
    display: "flex",
    alignItems: "center",
  },
}));

export const Actions = styled("div")({
  padding: "0 16px",
});

export const Cluster = styled("div")(({ theme }) => ({
  paddingLeft: 20,
  color: theme.common.colors.bg.colorBgBeige,
  userSelect: "none",
}));
