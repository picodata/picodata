import { styled } from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";

export const StyledInfoIcon = styled(InfoOutlinedIcon)(({ theme }) => ({
  width: 14,
  height: 14,
  marginLeft: 4,
  verticalAlign: "text-bottom",
  color: theme.common.colors.typography.colorTextGrey,
  cursor: "help",
}));
