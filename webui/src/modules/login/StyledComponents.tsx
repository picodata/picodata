import { styled } from "@mui/material";

export const StyleMain = styled("main")({
  gridArea: "main",
});

export const LoginContainer = styled("div")({
  display: "flex",
  flexDirection: "column",
  gap: "80px",
});

export const FormContainer = styled("div")({
  display: "flex",
  flexDirection: "column",
  gap: "24px",
});

export const Title = styled("h1")(({ theme }) => ({
  margin: 0,
  userSelect: "none",

  /* Heading/Heading 1 */
  fontStyle: "normal",
  fontWeight: 400,
  fontSize: "24px",
  lineHeight: "30px",
  /* identical to box height, or 125% */

  /* Text/Black */
  color: theme.common.colors.typography.colorTextBlack,

  /* Inside auto layout */
  flex: "none",
  order: 0,
  flexGrow: 0,
}));

export const Logo = styled("img")({
  userSelect: "none",
  height: "40px",
  width: "min-content",
});

export const StyleForm = styled("form")({
  display: "flex",
  flexDirection: "column",
  gap: "40px",
});

export const FormFields = styled("div")({
  display: "flex",
  flexDirection: "column",
  gap: "16px",
});

export const FormFieldPadded = styled("div")({
  marginTop: "8px",
});

export const FormActions = styled("div")({
  display: "flex",
  gap: "8px",
  " & > * ": {
    userSelect: "none",
    flexBasis: "200px",
  },
});

export const FormField = styled("div")({
  display: "flex",
  flexDirection: "column",
  gap: "8px",
});

export const PasswordToggle = styled("div")({
  display: "flex",
  alignItems: "center",
  padding: "10px",
  margin: "-10px",
  pointerEvents: "all",
  cursor: "pointer",
});

export const FormFieldError = styled("span")(({ theme }) => ({
  display: "inline-block",
  /* Body/Body 3 */
  fontStyle: "normal",
  fontWeight: "400",
  fontSize: "12px",
  lineHeight: "16px",
  /* identical to box height, or 133% */

  /* Fields/Text error */
  color: theme.common.colors.bgRed.colorBgRed,

  /* Inside auto layout */
  flex: "none",
  order: 0,
  flexGrow: 0,

  textWrapMode: "wrap",
  textWrapStyle: "stable",

  transition: "height 200ms ease",
  height: "calc-size(max-content, size)",
  // transition: "margin-top 200ms ease",
  marginTop: 0,

  "&:empty": {
    marginTop: "-8px",
  },
}));

export const FormFieldTitle = styled("span")(({ theme }) => ({
  width: "53px",
  height: "24px",
  fontStyle: "normal",
  fontWeight: 400,
  fontSize: "16px",
  lineHeight: "24px",
  /* identical to box height, or 150% */
  display: "flex",
  alignItems: "center",

  /* Text/Black */
  color: theme.common.colors.typography.colorTextBlack,

  /* Inside auto layout */
  flex: "none",
  order: 0,
  flexGrow: 0,
}));
