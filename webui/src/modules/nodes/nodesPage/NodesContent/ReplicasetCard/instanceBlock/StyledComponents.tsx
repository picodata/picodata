import { styled } from "@mui/material";

export const CardWrapper = styled("div")<{ $theme: "primary" | "secondary" }>(
  ({ $theme, theme }) => ({
    display: "flex",
    borderRadius: "4px",
    boxShadow: theme.common.variables.elevation1,
    position: "relative",
    overflow: "hidden",
    backgroundColor:
      $theme === "primary"
        ? theme.common.colors.grey.colorLightGrey
        : theme.common.colors.bg.colorBgWhite,
  })
);

export const Content = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  columnGap: "8px",
  flexGrow: 1,
  alignItems: "center",
  minHeight: "60px",
  padding: "0 16px",
});

export const InfoColumn = styled("div")({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  padding: "8px 0",
});
export const NameColumn = styled(InfoColumn)({
  alignItems: "flex-start",
  justifyContent: "center",
  maxWidth: "fit-content",
});

export const HiddenNameColumn = styled(NameColumn)({
  overflow: "hidden",
});

export const FailureDomainColumn = styled(InfoColumn)({
  flex: 1,
  maxWidth: "fit-content",
  overflow: "hidden",
});

export const HiddenFailureDomainColumn = styled(FailureDomainColumn)({
  overflow: "hidden",
});

export const TargetStateColumn = styled(InfoColumn)({
  minWidth: "95px",
});
export const CurrentStateColumn = styled(InfoColumn)({
  minWidth: "95px",
});
export const HiddenValue = styled(InfoColumn)({
  width: "100%",
  display: "grid",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
});
export const VersionColumn = styled(HiddenValue)({
  width: "auto !important",
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

export const JoinedColumn = styled("div")({
  transform: "translateX(74px)",
  display: "grid",
  alignItems: "start",
  gridTemplateColumns: "auto auto",
  columnGap: "16px",
  maxWidth: "max-content",
  "@ media (min-width: 1920px)": {
    columnGap: "32px",
  },
});

export const Value = styled("div")(({ theme }) => ({
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
export const StartValue = styled(ValueHidden)({
  textAlign: "start",
});
export const DomainValue = styled(Value)({
  display: "flex",
  width: "100%",
  justifyContent: "center",
});
export const TargetStateValue = styled(Value)({
  padding: 0,
});
export const CurrentStateValue = styled(Value)({
  padding: 0,
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
}));

export const LeaderBlock = styled(FollowerBlock)(({ theme }) => ({
  backgroundColor: theme.common.colors.notifications.colorNotificationSuccess,
}));
