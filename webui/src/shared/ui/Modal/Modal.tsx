import React from "react";
import { createPortal } from "react-dom";
import { SxProps } from "@mui/system/styleFunctionSx";

import { Body, Root } from "./StyledComponents";

type ModalProps = {
  children: React.ReactElement;
  bodySx?: SxProps;
  // containerClassName?: string;
  // bodyClassName?: string;
};

export const Modal: React.FC<ModalProps> = (props) => {
  const {
    // containerClassName, //ToDo
    // bodyClassName, //ToDo
    bodySx,
    children,
  } = props;

  return createPortal(
    <Root
      // className={cn(styles.wrapper, containerClassName)}//ToDo
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
      }}
    >
      <Body
        // className={cn(styles.body, bodyClassName)}//ToDo
        sx={bodySx}
      >
        {children}
      </Body>
    </Root>,
    document.body
  );
};
