import { Box, styled } from "@mui/material";

export const FilterValueContainer = styled(Box)(({ theme }) => ({
  cursor: "pointer",
  display: "flex",
  border: `1px solid ${theme.palette.grey.A400}`,
  "&:hover": {
    border: `1px solid ${theme.palette.primary.main}`,
  },
  borderRadius: 6,
}));
