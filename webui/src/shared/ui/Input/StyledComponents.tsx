import { styled } from "@mui/material";

export const Root = styled("div")<{ $isFocused: boolean }>(
  ({ theme, $isFocused }) => ({
    display: "flex",
    position: "relative",
    border: `1px solid ${theme.common.colors.fields.colorFieldStrokeNormal}`,
    borderRadius: "4px",
    height: "48px",
    "&:hover": {
      borderColor: theme.common.colors.fields.colorFieldTextMain,
    },
    "&:active": {
      borderColor: theme.common.colors.fields.colorFieldTextMain,
    },
    borderColor: $isFocused
      ? theme.common.colors.fields.colorFieldTextMain
      : "inherit",
  })
);

export const StyledInput = styled("input")(({ theme }) => ({
  outline: "none",
  border: "none",
  padding: "16px 20px",
  borderRadius: "4px",
  color: theme.common.colors.fields.colorFieldTextMain,
  width: "100%",
  "&::placeholder": {
    color: theme.common.colors.fields.colorFieldTextNormal,
  },
}));

export const IconContent = styled("div")({
  display: "flex",
  alignItems: "center",
  pointerEvents: "none",
  position: "absolute",
  right: "20px",
  top: "50%",
  transform: "translateY(-50%)",
});

export const RightIcon = styled("div")({
  display: "flex",
  alignItems: "center",
});
