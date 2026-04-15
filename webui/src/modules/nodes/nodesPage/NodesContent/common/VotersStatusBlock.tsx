import { Box, styled } from "@mui/material";
import { green } from "@mui/material/colors";
import LibraryAddCheckIcon from "@mui/icons-material/LibraryAddCheck";

import { useTranslation } from "../../../../../shared/intl";

import { StatusBlock } from "./ItemRoot";

const StyledLibraryAddCheckIcon = styled(LibraryAddCheckIcon)(() => ({
  fill: green[600],
  width: 14,
  height: 14,
}));
export const Root = styled(StatusBlock)({
  backgroundColor: green[100],
  color: green[800],
  fontSize: 12,
});

export const VotersStatusBlock = () => {
  const { translation } = useTranslation();
  const tierTranslations = translation.pages.instances.list.tierCard;
  return (
    <Root>
      <Box>
        <StyledLibraryAddCheckIcon />
      </Box>
      <Box>{tierTranslations.statuses.voter.label}</Box>
    </Root>
  );
};
