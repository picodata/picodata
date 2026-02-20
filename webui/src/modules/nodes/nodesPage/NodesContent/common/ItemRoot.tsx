import { Box, styled } from "@mui/material";

export const ITEM_GRID_COLUMNS_SCHEMA =
  "180px 1fr 120px 120px 160px 120px 1fr 1fr 60px";

const BORDER_RADIUS = "6px";

export const ItemRoot = styled(Box)({
  width: "100%",
  display: "grid",
  gridTemplateColumns: ITEM_GRID_COLUMNS_SCHEMA,
  overflow: "hidden",
  "& > *": {
    overflow: "hidden",
  },
});
export const CellLabel = styled(Box)(({ theme }) => ({
  fontSize: "12px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
}));
export const CellValue = styled(Box)({
  fontSize: "14px",
  lineHeight: "16px",
  overflow: "hidden",
});

export const Cell = styled(Box)({
  cursor: "pointer",
});
export const CommonCell = styled(Cell)<{
  $position?: "left" | "right";
  $isOpen?: boolean;
}>(({ $position, $isOpen, theme }) => ({
  backgroundColor: theme.common.colors.bg.colorBgGrey,
  ...($position
    ? {
        borderRadius: `${$position === "right" ? 0 : BORDER_RADIUS} ${
          $position === "right" ? BORDER_RADIUS : 0
        } ${$position === "right" && !$isOpen ? BORDER_RADIUS : 0} ${
          $position === "left" && !$isOpen ? BORDER_RADIUS : 0
        }`,
      }
    : {}),
}));
export const ContentCell = styled(Cell)({
  padding: 10,
});
export const ContentFlexCell = styled(ContentCell)({
  display: "flex",
  flexDirection: "column",
  gap: 6,
});
export const ContentFlexCenteredCell = styled(ContentFlexCell)({
  alignItems: "center",
});

export const CapacityProgressCell = styled(ContentFlexCenteredCell)({
  paddingTop: 17,
});

export const Ellipsis = styled(Box)({
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
});

export const Background = styled(Box)<{
  $variant: "white" | "gray";
  $withBottomRadius: boolean;
}>(({ theme, $variant, $withBottomRadius }) => ({
  backgroundColor:
    $variant === "white"
      ? theme.common.colors.bg.colorBgWhite
      : theme.common.colors.bg.colorBgGrey,
  ...($withBottomRadius
    ? {
        borderRadius: "0 0 10px 10px",
      }
    : {}),
}));
