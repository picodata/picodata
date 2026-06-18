import { Box, styled } from "@mui/material";

import { ModalToolBarProps } from "../model";

export const BaseToolBar = styled(Box)({
  display: "grid",
  gridTemplateColumns: "min-content 1fr min-content",
});

export const ToolBarColumn = styled(Box)({
  overflow: "hidden",
  display: "flex",
  alignItems: "center",
});
export const ToolBarActionsColumn = styled(ToolBarColumn)({
  gap: 6,
});

export const ToolBar = ({
  leftContent,
  centerContent,
  rightContent,
}: ModalToolBarProps) => {
  return (
    <BaseToolBar>
      <ToolBarColumn>{leftContent}</ToolBarColumn>
      <ToolBarColumn>{centerContent}</ToolBarColumn>
      <ToolBarColumn>{rightContent}</ToolBarColumn>
    </BaseToolBar>
  );
};
