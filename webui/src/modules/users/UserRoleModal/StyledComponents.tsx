import { styled, SxProps } from "@mui/material";

export const TitleWrapper = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  marginBottom: "16px",
});

export const TitleText = styled("span")({
  fontSize: "16px",
  fontWeight: 500,
  display: "flex",
  alignItems: "center",
});

export const modalBodySx: SxProps = {
  width: "510px",
  padding: "40px",
  height: "auto",
};

export const CloseElement = styled("div")({
  cursor: "pointer",
  width: "32px",
  height: "32px",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
});

export const BlockElement = styled("div")({
  marginTop: "24px",
  "&:first-child": {
    marginTop: 0,
  },
});

export const Label = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextGrey,
  fontSize: "14px",
  lineHeight: "16px",
  marginBottom: "8px",
}));
export const Value = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.colorTextBlack,
  fontSize: "14px",
  lineHeight: "16px",
}));
export const Divider = styled("div")(({ theme }) => ({
  backgroundColor: theme.common.colors.secondary.colorSecondarySelected,
  width: "1px",
  height: "16px",
  margin: "0 8px",
}));

export const Row = styled("div")({
  display: "flex",
  alignItems: "center",
});

export const DisabledValue = styled("div")(({ theme }) => ({
  color: theme.common.colors.typography.$colorTextDisabled,
  fontSize: "14px",
  lineHeight: "16px",
}));

export const DisabledContainer = styled("div")(({ theme }) => ({
  backgroundColor: theme.common.colors.fields.colorFieldDisabled,
  width: "100%",
  height: "48px",
  padding: "16px 20px 16px 20px",
  borderRadius: "4px",
  color: theme.common.colors.typography.$colorTextDisabled,
  lineHeight: "16px",
  fontSize: "14px",
}));

export const ButtonsRow = styled("div")({
  display: "flex",
  alignItems: "center",
  flexWrap: "wrap",
  marginBottom: "4px",
});

export const ButtonElement = styled("div")<{ $isActive: boolean }>(
  ({ theme, $isActive }) => ({
    display: "flex",
    alignItems: "center",
    padding: "6px 16px 6px 16px",
    borderRadius: "4px",
    border: `1px solid ${
      $isActive
        ? theme.common.colors.typography.colorTextOrange
        : theme.common.colors.bg.colorBgGray
    }`,
    fontSize: "12px",
    lineHeight: "16px",
    background: $isActive
      ? "linear-gradient(0deg, #fdfdfd, #fdfdfd), linear-gradient(0deg, #f7941d, #f7941d)"
      : "linear-gradient(0deg, #e8e8e9, #e8e8e9) linear-gradient(0deg, #fdfdfd, #fdfdfd)",
    marginRight: "16px",
    cursor: "pointer",
    marginBottom: "4px",
    "&:last-child": {
      marginRight: 0,
    },
  })
);

export const MultiSelect = styled("div")(({ theme }) => ({
  width: "100%",
  padding: "16px",
  borderRadius: "4px",
  border: `1px solid ${theme.common.colors.fields.colorFieldStrokeNormal}`,
  display: "flex",
  flexWrap: "wrap",
  alignItems: "center",
  paddingBottom: "8px",
  overflowY: "auto",
  maxHeight: "100px",
}));

export const MultiItem = styled("div")(({ theme }) => ({
  fontSize: "14px",
  lineHeight: "16px",
  display: "flex",
  alignItems: "center",
  marginBottom: "8px",
  color: theme.common.colors.typography.colorTextBlack,
}));

export const Footer = styled("div")({
  display: "flex",
  justifyContent: "flex-end",
  marginTop: "40px",
});
