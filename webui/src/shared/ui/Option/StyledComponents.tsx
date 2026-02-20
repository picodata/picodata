import { styled } from "@mui/material";

export const Root = styled("div")<{ $isSelected: boolean }>(
  ({ theme, $isSelected }) => ({
    padding: "12px 16px",
    display: "flex",
    justifyContent: "flex-start",
    cursor: "pointer",
    fontSize: "14px",
    lineHeight: "16px",
    fontWeight: 400,
    color: $isSelected
      ? theme.common.colors.typography.colorTextOrange
      : theme.common.colors.typography.colorTextBlack,
    "&:hover": {
      backgroundColor: theme.common.colors.fields.colorFieldForm,
    },
  })
);
