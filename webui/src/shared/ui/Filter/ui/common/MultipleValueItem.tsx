import { Box, styled } from "@mui/material";

export const MultipleValues = styled(Box)({
  display: "flex",
  gap: 4,
  paddingRight: 3,
});
export const MultipleValueItem = styled(Box)(({ theme }) => ({
  padding: "2px 4px",
  fontSize: 12,
  border: `1px solid ${theme.palette.primary.main}`,
  borderRadius: 4,
}));
