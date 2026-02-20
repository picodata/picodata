import { styled } from "@mui/material";
import { CSSProperties } from "react";

export const CardWrapper = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  borderRadius: "4px",
  backgroundColor: theme.common.colors.bg.colorBgGrey,
  boxShadow: theme.common.variables.elevation1,
  cursor: "pointer",
  transition: "background-color .2s ease-out",
  "&:hover": {
    backgroundColor: "#e0e0e2",
  },
}));

export const Content = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  columnGap: "16px",
  padding: "8px 16px",
});

export const InfoColumn = styled("div")({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  whiteSpace: "nowrap",
});

export const HiddenInfoColumn = styled(InfoColumn)({
  overflow: "hidden",
});

export const NameColumn = styled(HiddenInfoColumn)({
  flex: "1 179px",
  alignItems: "flex-start",
});

export const ServicesColumn = styled(HiddenInfoColumn)({
  flex: "1 167px",
});

export const ReplicasetsColumn = styled(InfoColumn)({
  flex: "1 96px",
});

export const InstancesColumn = styled(InfoColumn)({
  flex: "1 78px",
});

export const RfColumn = styled(InfoColumn)({
  flex: "1 96px",
  whiteSpace: "nowrap",
});

export const BucketCountColumn = styled(InfoColumn)({
  flex: "1 78px",
});

export const CanVoterColumn = styled(InfoColumn)({
  flex: "1 60px",
});

export const CapacityColumn = styled(InfoColumn)({
  flex: "1 360px",
  alignItems: "flex-start",
  padding: "8px 8px",
});

export const ChevronColumn = styled(InfoColumn)({
  flexBasis: "64px",
  justifyContent: "center",
  padding: "4px 16px",
});

export const Label = styled("div")(({ theme }) => ({
  fontSize: "12px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "4px 8px",
}));

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

export const ServicesValue = styled(HiddenInfoValue)({
  textAlign: "center",
});

export const ReplicasetsWrapper = styled("div")({
  padding: "0 8px",
  paddingBottom: "8px",
  display: "flex",
  flexDirection: "column",
  rowGap: "8px",
});

export const chevronIconStyle: CSSProperties = {
  color: "#050505",
};

export const chevronIconIsOpenStyle: CSSProperties = {
  color: "#050505",
  transform: "rotate(-180deg)",
};
