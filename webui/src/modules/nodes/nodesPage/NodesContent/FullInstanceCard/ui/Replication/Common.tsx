import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { StyledLink } from "../common";

export const Label = styled(StyledLink)({
  color: grey[500],
  fontSize: 13,
});
export const Value = styled(Box)({
  fontSize: 13,
});

export const Message = styled(Box)({
  color: grey[500],
});
export const FlexRow = styled(Box)({
  display: "flex",
});
