import { styled } from "@mui/material";
import MuiCloseIcon from "@mui/icons-material/Close";
import FullscreenIcon from "@mui/icons-material/Fullscreen";
import FullscreenExitIcon from "@mui/icons-material/FullscreenExit";

import { ModalHeaderHeaderProps } from "../model";

import { ToolBar, ToolBarActionsColumn } from "./ToolBar";
import { PaddingBlock } from "./Common";
const Root = styled(PaddingBlock)({
  paddingTop: 10,
});

const CloseIcon = styled(MuiCloseIcon)(({ theme }) => ({
  color: theme.palette.primary.main,
  cursor: "pointer",
  width: 24,
  height: 24,
  "&:hover": {
    color: theme.palette.primary.dark,
  },
}));
const StyledFullscreenIcon = styled(FullscreenIcon)(({ theme }) => ({
  color: theme.palette.primary.main,
  cursor: "pointer",
  width: 24,
  height: 24,
  "&:hover": {
    color: theme.palette.primary.dark,
  },
}));
const StyledFullscreenExitIcon = styled(FullscreenExitIcon)(({ theme }) => ({
  color: theme.palette.primary.main,
  cursor: "pointer",
  width: 24,
  height: 24,
  "&:hover": {
    color: theme.palette.primary.dark,
  },
}));

export const ModalHeader = ({
  rightContent,
  onCloseClick,
  onFullScreenClick,
  fullScreen,
  ...props
}: ModalHeaderHeaderProps) => {
  return (
    <Root>
      <ToolBar
        rightContent={
          rightContent ?? (
            <ToolBarActionsColumn>
              {fullScreen ? (
                <StyledFullscreenExitIcon onClick={onFullScreenClick} />
              ) : (
                <StyledFullscreenIcon onClick={onFullScreenClick} />
              )}
              <CloseIcon onClick={onCloseClick} />
            </ToolBarActionsColumn>
          )
        }
        {...props}
      />
    </Root>
  );
};
