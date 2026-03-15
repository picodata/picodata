import { Box, styled } from "@mui/material";

import {
  Background,
  Cell,
  CommonCell,
  ContentFlexCenteredCell,
  ItemRoot,
} from "../../common";

export const Content = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  columnGap: "8px",
  flexGrow: 1,
  alignItems: "center",
  minHeight: "60px",
  padding: "0 16px",
});

export const Label = styled("div")<{ $alignLeft?: boolean }>(
  ({ $alignLeft, theme }) => ({
    fontSize: "12px",
    fontStyle: "normal",
    fontWeight: 400,
    lineHeight: "16px",
    textAlign: $alignLeft ? "left" : "center",
    textOverflow: "ellipsis",
    overflow: "hidden",
    maxHeight: "24px",
    color: theme.common.colors.typography.colorTextBlack,
    padding: "4px 0",
  })
);

export const Value = styled(Box)(({ theme }) => ({
  fontSize: "14px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "4px 0",
  textAlign: "center",
}));
export const ValueHidden = styled(Value)({
  width: "100%",
  display: "grid",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
});

export const DomainValue = styled(Value)({
  display: "flex",
  width: "100%",
  justifyContent: "center",
  gap: 4,
  overflow: "hidden",
});

export const FollowerBlock = styled(Value)(({ theme }) => ({
  width: "16px",
  backgroundColor: "transparent",
  fontSize: "10px",
  lineHeight: "16px",
  fontWeight: 400,
  color: theme.common.colors.typography.colorTextGrey,
  textTransform: "uppercase",
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  writingMode: "vertical-lr",
  textOrientation: "mixed",
  marginRight: 10,
}));

export const LeaderBlock = styled(FollowerBlock)(({ theme }) => ({
  backgroundColor: theme.common.colors.notifications.colorNotificationSuccess,
  marginRight: 10,
}));

export const InstanceItemRoot = styled(ItemRoot)<{ $fromReplicaset: boolean }>(
  ({ $fromReplicaset, theme }) => ({
    borderRadius: 6,
    backgroundColor: $fromReplicaset
      ? theme.common.colors.bg.colorBgWhite
      : theme.common.colors.bg.colorBgGrey,
    ...(!$fromReplicaset
      ? {
          backgroundColor: theme.common.colors.bg.colorBgGrey,
          borderBottom: `4px solid ${theme.common.colors.bg.colorBgWhite}`,
        }
      : {}),
  })
);

export const InstanceCell = styled(Cell)<{ $fromReplicaset: boolean }>(
  ({ theme, $fromReplicaset }) => ({
    backgroundColor: $fromReplicaset
      ? theme.common.colors.bg.colorBgGrey
      : theme.common.colors.bg.colorBgWhite,
  })
);

export const InstanceNameCell = styled(CommonCell)<{
  $fromReplicaset: boolean;
}>(({ theme, $fromReplicaset }) => ({
  backgroundColor: $fromReplicaset
    ? theme.common.colors.bg.colorBgWhite
    : theme.common.colors.bg.colorBgGrey,
  width: "100%",
  height: "100%",
  display: "grid",
  gridTemplateColumns: "min-content 1fr",
}));

export const ContentFlexCenteredNameCell = styled(ContentFlexCenteredCell)<{
  $fromReplicaset: boolean;
}>(({ $fromReplicaset }) =>
  $fromReplicaset
    ? {
        alignItems: "flex-start",
        justifyContent: "center",
      }
    : {}
);

export const InstanceBackground = styled(Background)<{
  $withBottomPadding: boolean;
}>(({ $withBottomPadding }) => ({
  padding: `0 10px ${$withBottomPadding ? "10px" : "0"} 10px`,
}));

export const InstanceBackgroundInner = styled(Background)<{
  $withBottomPadding: boolean;
  $fromReplicaset: boolean;
}>(({ $withBottomPadding, $fromReplicaset }) => {
  const bottomPadding = $withBottomPadding ? "10px" : "0";
  return {
    padding: $fromReplicaset ? `4px 10px ${bottomPadding} 10px` : "unset",
  };
});

export const VersionRoot = styled(Box)({
  display: "grid",
  gridTemplateColumns: "1fr max-content",
});
