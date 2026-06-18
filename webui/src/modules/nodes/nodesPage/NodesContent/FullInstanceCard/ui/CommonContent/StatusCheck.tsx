import { styled } from "@mui/material";
import { grey } from "@mui/material/colors";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";

const StyledCheckBoxIcon = styled(CheckBoxIcon)(({ theme }) => ({
  width: 18,
  height: 18,
  color: theme.palette.success.main,
}));
const StyledCCheckBoxOutlineBlankIcon = styled(CheckBoxOutlineBlankIcon)({
  width: 18,
  height: 18,
  color: grey[400],
});

type StatusCheckProps = {
  checked?: boolean;
};

export const StatusCheck = ({ checked }: StatusCheckProps) => {
  return checked ? <StyledCheckBoxIcon /> : <StyledCCheckBoxOutlineBlankIcon />;
};
