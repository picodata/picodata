import { Box, Modal as MuiModal, styled } from "@mui/material";
import { useState } from "react";

import { ModalProps } from "../model";

import { ModalHeader } from "./Header";
import { ModalFooter } from "./Footer";

const Root = styled(Box)<{ $fullScreen: boolean }>(({ $fullScreen }) => ({
  overflow: "hidden",
  display: "grid",
  gridTemplateRows: "min-content minmax(0, 1fr) min-content",
  background: "white",
  position: "absolute",
  top: "50%",
  left: "50%",
  transform: "translate(-50%, -50%)",
  borderRadius: 10,

  width: "fit-content",
  maxWidth: "90vw",

  height: "fit-content",
  maxHeight: "80vh",

  ...($fullScreen && {
    width: "100vw !important",
    maxWidth: "100vw !important",
    height: "100vh !important",
    maxHeight: "100vh !important",
    borderRadius: 0,
  }),
}));
const Row = styled(Box)({
  overflow: "hidden",
});

export const Modal = ({
  header,
  footer,
  muiModalProps = {},
  onClose,
  open,
  children,
  title,
  sx,
}: ModalProps) => {
  const [fullScreen, setFullScreen] = useState(false);
  const changeFullScreenHandler = () => {
    setFullScreen((fs) => !fs);
  };
  return (
    <MuiModal open={open} onClose={onClose} {...muiModalProps}>
      <Root $fullScreen={fullScreen} sx={sx}>
        <Row>
          {header ?? (
            <ModalHeader
              centerContent={title}
              onCloseClick={onClose}
              onFullScreenClick={changeFullScreenHandler}
              fullScreen={fullScreen}
            />
          )}
        </Row>
        <Row>{children}</Row>
        <Row>{footer ?? <ModalFooter onOkClickHandler={onClose} />}</Row>
      </Root>
    </MuiModal>
  );
};
