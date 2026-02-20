import { Box, styled } from "@mui/material";
import { CSSProperties } from "react";

import {
  Background,
  CapacityProgressCell,
  ContentFlexCenteredCell,
  ITEM_GRID_COLUMNS_SCHEMA,
  ItemRoot,
} from "../common";

export const Content = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  columnGap: "16px",
  padding: "8px 16px",
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

export const TierContentFlexCenteredCell = styled(ContentFlexCenteredCell)(
  ({ theme }) => ({
    backgroundColor: theme.common.colors.bg.colorBgGrey,
  })
);
export const TierCapacityProgressCell = styled(CapacityProgressCell)(
  ({ theme }) => ({
    backgroundColor: theme.common.colors.bg.colorBgGrey,
  })
);
export const ServicesList = styled(Box)({
  display: "flex",
  gap: 6,
  flexDirection: "column",
  alignItems: "center",
});

export const TierBackground = styled(Background)<{
  $withPaddingBottom: boolean;
  $withPaddingTop: boolean;
}>(({ $withPaddingBottom, $withPaddingTop }) => ({
  padding: `${$withPaddingTop ? "10px" : "0"} 10px ${
    $withPaddingBottom ? "10px" : "0"
  } 10px`,
}));
export const TierItemRoot = styled(ItemRoot)<{ $withBorderRadius: boolean }>(
  ({ $withBorderRadius, theme }) => {
    const bottomRadius = $withBorderRadius ? "6px" : 0;
    return {
      borderRadius: `6px 6px ${bottomRadius} ${bottomRadius}`,
      gridTemplateColumns: `10px ${ITEM_GRID_COLUMNS_SCHEMA} 10px`,
      backgroundColor: theme.common.colors.bg.colorBgGrey,
    };
  }
);
