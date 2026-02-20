import { styled, SxProps } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  borderRadius: "4px",
  backgroundColor: theme.common.colors.bg.colorBgTableLine,
  boxShadow: theme.common.variables.elevation1,
  cursor: "pointer",
  "&:hover": {
    backgroundColor: theme.common.colors.bg.colorBgTableLine,
  },
}));

export const Content = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  columnGap: "16px",
  padding: "12px 24px",
});

export const Column = styled("div")({
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  alignItems: "center",
});

export const NameColumn = styled(Column)({
  alignItems: "flex-start",
  width: "18%",
});
export const RoleColumn = styled(Column)({
  width: "16%",
});

export const PrivilegesForTables = styled(Column)({
  width: "22%",
});
export const PrivilegesForUsers = styled(Column)({
  width: "22%",
});
export const PrivilegesForRoles = styled(Column)({
  width: "22%",
});

export const Label = styled("div")(({ theme }) => ({
  fontSize: "12px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "2px 8px",
}));

export const InfoValue = styled("div")(({ theme }) => ({
  fontSize: "14px",
  fontStyle: "normal",
  fontWeight: 400,
  lineHeight: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  padding: "2px 8px",
  maxWidth: "100%",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
}));

export const textSx: SxProps = {
  height: "18px",
};
