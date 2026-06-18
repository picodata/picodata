import { Box, styled } from "@mui/material";
import FolderIcon from "@mui/icons-material/Folder";
import { grey } from "@mui/material/colors";

const Root = styled(Box)({
  display: "flex",
  alignItems: "center",
  gap: 6,
});
const StyledFolderIcon = styled(FolderIcon)({
  color: grey[600],
  width: 18,
  height: 18,
});
type FolderProps = {
  value?: string | null;
};
export const FolderName = ({ value }: FolderProps) => {
  return (
    <Root>
      <StyledFolderIcon />
      <Box>{value}</Box>
    </Root>
  );
};
