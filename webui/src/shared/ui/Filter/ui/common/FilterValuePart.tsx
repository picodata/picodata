import { Box, styled } from "@mui/material";

export const FilterValuePart = styled(Box)({
  display: "flex",
  alignItems: "center",
  padding: "0 6px",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
});

export const RestFilterValuePart = styled(FilterValuePart)(({ theme }) => ({
  background: theme.palette.grey.A200,
  "&:hover": {
    background: theme.palette.primary.light,
  },
  maxWidth: "unset",
}));
