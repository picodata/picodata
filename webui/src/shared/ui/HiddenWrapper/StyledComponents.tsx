import { styled } from "@mui/material";

export const Root = styled("div")<{ $twoLine?: boolean }>(({ $twoLine }) => ({
  fontSize: "14px",
  lineHeight: "16px",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
  ...($twoLine
    ? {
        whiteSpace: "normal",
        display: "-webkit-box",
        "-webkit-line-clamp": 2,
        "-webkit-box-orient": "vertical",
      }
    : {}),
}));

export const Overlay = styled("div")(({ theme }) => ({
  maxWidth: "320px",
  fontSize: "14px",
  whiteSpace: "initial",
  wordWrap: "break-word",
  display: "flex",
  alignItems: "center",
  flexWrap: "wrap",
  color: theme.common.colors.typography.colorTextBlack,
}));
