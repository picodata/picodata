import { Button, styled } from "@mui/material";

import { ModalFooterProps } from "../model";
import { useTranslation } from "../../../intl";

import { ToolBar, ToolBarActionsColumn } from "./ToolBar";
import { PaddingBlock } from "./Common";

const Root = styled(PaddingBlock)({
  paddingBottom: 10,
});

export const ModalFooter = ({ onOkClickHandler }: ModalFooterProps) => {
  const { translation } = useTranslation();
  return (
    <Root>
      <ToolBar
        rightContent={
          <ToolBarActionsColumn>
            <Button onClick={onOkClickHandler} variant={"contained"}>
              {translation.common.ok}
            </Button>
          </ToolBarActionsColumn>
        }
      />
    </Root>
  );
};
