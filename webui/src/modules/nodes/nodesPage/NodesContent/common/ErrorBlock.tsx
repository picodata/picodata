import { PropsWithChildren } from "react";
import { Box, styled } from "@mui/material";
import ErrorOutlineOutlinedIcon from "@mui/icons-material/ErrorOutlineOutlined";

const Root = styled(Box)(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  gap: 4,
  fontSize: 14,
  whiteSpace: "nowrap",
  color: theme.palette.error.main,
}));

const StyledErrorOutlineOutlinedIcon = styled(ErrorOutlineOutlinedIcon)({
  width: 14,
  height: 14,
});
export const ErrorBlock = ({ children }: PropsWithChildren) => {
  return (
    <Root>
      <StyledErrorOutlineOutlinedIcon />
      <Box>{children}</Box>
    </Root>
  );
};
