import { styled } from "@mui/material";

export const Root = styled("div")({
  width: "100%",
});

export const Content = styled("div")({
  width: "100%",
  display: "flex",
});

export const Text = styled("div")<{
  $size: "small" | "medium";
  $theme: "primary" | "secondary";
}>(({ theme, $size, $theme }) => ({
  color: theme.common.colors.typography.colorTextBlack,
  whiteSpace: "nowrap",
  fontWeight: $theme === "secondary" ? 400 : 500,
  fontSize: $size === "small" ? "12px" : "16px",
  lineHeight: $size === "small" ? "16px" : "24px",
  marginTop: $size === "medium" ? "-3px" : "unset",
}));

export const ProgressLineContainer = styled("div")<{
  $size: "small" | "medium";
}>(({ $size }) => ({
  marginLeft: $size === "medium" ? "8px" : "4px",
  paddingTop: $size === "medium" ? 0 : "2px",
  width: "100%",
  ...($size === "medium"
    ? {
        paddingBottom: 0,
      }
    : {}),
}));

export const ProgressLineInfo = styled("div")<{
  $size: "small" | "medium";
}>(({ $size }) => ({
  display: "flex",
  justifyContent: "space-between",

  ...($size === "medium"
    ? {
        marginTop: "2px",
      }
    : {}),
}));

export const Label = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextGrey,
  fontSize: "12px",
  fontWeight: 400,
  lineHeight: "16px",
}));
