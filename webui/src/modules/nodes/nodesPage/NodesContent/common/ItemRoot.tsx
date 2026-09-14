import { Box, styled, Tooltip } from "@mui/material";
import { green } from "@mui/material/colors";
import LibraryAddCheckIcon from "@mui/icons-material/LibraryAddCheck";
import { PropsWithChildren } from "react";

import { Leader } from "shared/icons";

export const ITEM_GRID_COLUMNS_SCHEMA =
  "minmax(110px, 1.4fr) minmax(70px, 1fr) minmax(70px, 0.6fr) minmax(70px, 0.6fr) minmax(95px, 0.8fr) minmax(130px, 1.6fr) minmax(90px, 0.7fr) minmax(125px, 1.8fr) 44px";

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
const StyledCellLabel = styled(Box)(({ theme }) => ({
  maxWidth: "100%",
  fontSize: "12px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  overflow: "hidden",
  whiteSpace: "nowrap",
  textOverflow: "ellipsis",
}));

export const CellLabel = ({ children, ...rest }: PropsWithChildren) => {
  const label = <StyledCellLabel {...rest}>{children}</StyledCellLabel>;

  // Only plain text labels get a truncation tooltip - composite children
  // (e.g. a label next to an info icon) already manage their own hints.
  if (typeof children !== "string") {
    return label;
  }

  return <Tooltip title={children}>{label}</Tooltip>;
};

export const CellValue = styled(Box)({
  fontSize: "14px",
  lineHeight: "16px",
  overflow: "hidden",
});
export const LinkCellValue = styled(CellValue)(({ theme }) => ({
  cursor: "pointer",
  "&:hover": {
    color: theme.palette.primary.main,
  },
}));

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
export const CellCenter = styled(ContentCell)({
  height: "100%",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
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

export const StyledLeaderIcon = styled(Leader)({
  width: 16,
  height: 16,
});
export const StyledVoterIcon = styled(LibraryAddCheckIcon)({
  fill: green[600],
  width: 16,
  height: 16,
});

export const RaftLeaderNameCell = styled(CellValue)({
  display: "grid",
  gridTemplateColumns: "min-content min-content 1fr",
  gap: 3,
  "& > *": {
    display: "flex",
    alignItems: "center",
    overflow: "hidden",
  },
});

export const StatusBlock = styled(Box)({
  display: "flex",
  borderRadius: 6,
  gap: 4,
  padding: "4px 8px",
  whiteSpace: "nowrap",
  "& > *": {
    display: "flex",
    alignItems: "center",
  },
  color: "white",
});
