import { styled } from "@mui/material";
import { CSSProperties } from "react";

import { Background, CellValue, CommonCell, ItemRoot } from "../common";

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

export const chevronIconStyle: CSSProperties = {
  color: "#050505",
};

export const chevronIconIsOpenStyle: CSSProperties = {
  color: "#050505",
  transform: "rotate(-180deg)",
};

export const ReplicasetIconCell = styled(CommonCell)(({ theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgWhite,
  width: "100%",
  height: "100%",
}));

export const ReplicasetItemRoot = styled(ItemRoot)<{
  $withBotomRadius: boolean;
}>(({ theme, $withBotomRadius }) => {
  const bottomRadius = $withBotomRadius ? "6px" : "0";
  return {
    backgroundColor: theme.common.colors.bg.colorBgWhite,
    borderRadius: `6px 6px ${bottomRadius} ${bottomRadius}`,
  };
});

export const ReplicasetBackground = styled(Background)<{
  $withBottomPadding: boolean;
}>(({ $withBottomPadding }) => ({
  padding: `0 10px ${$withBottomPadding ? "10px" : "0"} 10px`,
}));

export const ReplicasetInnerBackground = styled(Background)<{
  $withBottomPadding: boolean;
}>(({ $withBottomPadding }) => ({
  padding: ` 6px 10px ${$withBottomPadding ? "10px" : 0} 10px`,
}));

export const InstanceCountContainer = styled(CellValue)<{
  $highlight: boolean;
}>(({ $highlight, theme }) =>
  $highlight
    ? {
        color: theme.palette.error.main,
      }
    : {}
);
