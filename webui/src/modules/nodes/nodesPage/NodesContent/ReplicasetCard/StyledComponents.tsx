import { styled, SxProps } from "@mui/material";
import { CSSProperties } from "react";

export const CardWrapper = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  borderRadius: "4px",
  backgroundColor: theme.common.colors.bg.colorBgBeige,
  cursor: "pointer",
}));

export const CardWrapperTheme = styled(CardWrapper)<{
  $theme: "primary" | "secondary";
}>(({ $theme, theme }) => ({
  backgroundColor:
    $theme === "primary"
      ? theme.common.colors.bg.colorBgBeige
      : theme.common.colors.bg.colorBgWhite,
}));

export const Content = styled("div")<{
  $theme: "primary" | "secondary";
}>(({ $theme }) => ({
  display: "flex",
  justifyContent: "space-between",
  padding: `8px ${$theme === "secondary" ? "8px" : "16px"}`,
  columnGap: $theme === "secondary" ? "16px" : "24px",
  ...($theme === "secondary"
    ? {
        boxShadow: "0px -1px 0px 0px #e4e4ef inset",
      }
    : {}),
}));
export const InstancesWrapper = styled("div")<{
  $theme: "primary" | "secondary";
}>(({ $theme }) => ({
  ...($theme === "secondary"
    ? {
        paddingLeft: "16px",
      }
    : {
        padding: "0 8px",
        paddingBottom: " 8px",
        rowGap: "8px",
      }),
}));

export const instancesCardWrapperSx: SxProps = {
  borderRadius: 0,
  boxShadow: "0px -1px 0px 0px #e4e4ef inset",
  "&:last-child": {
    boxShadow: "none",
  },
};

export const InfoColumn = styled("div")({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
});

export const HiddenColumn = styled(InfoColumn)({
  overflow: "hidden",
});

export const NameColumn = styled(HiddenColumn)({
  flex: "1 360px",
  alignItems: "flex-start",
});

export const InstancesColumn = styled(InfoColumn)({
  flex: "1 96px",
});

export const StateColumn = styled("div")({
  flex: "1 135px",
  whiteSpace: "nowrap",
});

export const InfoStateColumn = styled(InfoColumn)({
  flex: "1 135px",
  whiteSpace: "nowrap",
});

export const CapacityColumn = styled(InfoColumn)({
  flex: "1 288px",
  alignItems: "flex-start",
  padding: "8px 8px",
});

export const ChevronColumn = styled(InfoColumn)({
  flexBasis: "64px",
  justifyContent: "center",
});

export const InfoValue = styled("div")(({ theme }) => ({
  fontSize: "14px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "4px 8px",
}));

export const HiddenInfoValue = styled(InfoValue)({
  width: "100%",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
});

export const StateInfoValue = styled(InfoValue)({
  padding: 0,
});

export const Label = styled("div")(({ theme }) => ({
  fontSize: "12px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "4px 8px",
}));

export const chevronIconStyle: CSSProperties = {
  color: "#050505",
};

export const chevronIconIsOpenStyle: CSSProperties = {
  color: "#050505",
  transform: "rotate(-180deg)",
};
