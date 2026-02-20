import { styled, SxProps } from "@mui/material";

export const containerSx: SxProps = {
  padding: "8px 24px",
  display: "flex",
  justifyContent: "space-between",
};

export const CapacityInfoColumn = styled("div")({
  flexBasis: "23%",
  padding: "0 24px",
  display: "flex",
  flexDirection: "column",
  gap: "4px",
  alignItems: "flex-start",
  flexShrink: 0,
});

export const ColumnName = styled("div")(({ theme }) => ({
  fontSize: "16px",
  fontWeight: 500,
  lineHeight: "30px",
  color: theme.common.colors.typography.colorTextBlack,
}));

export const CapacityWrapper = styled("div")({
  display: "flex",
});

export const RightContainer = styled("div")({
  display: "flex",
  justifyContent: "flex-end",
  alignItems: "center",
  flexGrow: 1,
  columnGap: "64px",
});

export const RightColumn = styled("div")({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
});

export const ColumnContent = styled("div")({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  justifyContent: "center",
});

export const ColumnValue = styled("div")<{ $isWarning?: boolean }>(
  ({ theme, $isWarning }) => ({
    fontSize: "16px",
    fontStyle: "normal",
    fontWeight: $isWarning ? "bold" : 500,
    lineHeight: "24px",
    color: $isWarning
      ? theme.common.colors.bgRed.colorBgRed
      : theme.common.colors.typography.colorTextBlack,
  })
);

export const ColumnLabel = styled("div")<{ $isWarning?: boolean }>(
  ({ theme, $isWarning }) => ({
    fontSize: "14px",
    fontStyle: "normal",
    fontWeight: $isWarning ? "bold" : 400,
    lineHeight: "16px",
    marginTop: "6px",
    color: $isWarning
      ? theme.common.colors.bgRed.colorBgRed
      : theme.common.colors.typography.colorTextBlack,
    textAlign: "center",
  })
);

export const InstancesBlock = styled("div")({
  display: "flex",
  alignItems: "center",
  columnGap: "24px",
});
