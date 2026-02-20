import React, { useRef, useState } from "react";

import { useOutsideClickEvent } from "shared/react/hooks/useOutsideClickEvent";

import { Button, ButtonProps } from "../Button/Button";
import { ModalBody, ModalBodyProps } from "../Modal/ModalBody/ModalBody";

import { Root } from "./StyledComponents";

type ButtonModalProps = {
  buttonProps: ButtonProps;
  modalProps: Omit<ModalBodyProps, "onClose" | "children"> & {
    onClose?: () => void;
  };
  children: ModalBodyProps["children"];
};

export const ButtonModal: React.FC<ButtonModalProps> = (props) => {
  const { buttonProps, modalProps, children } = props;
  const containerRef = useRef<HTMLDivElement>(null);

  const [modalIsOpen, setModalIsOpen] = useState(false);

  useOutsideClickEvent(containerRef, () => {
    setModalIsOpen(false);
  });

  return (
    <Root ref={containerRef}>
      <Button
        size="normal"
        onClick={(event) => {
          event.stopPropagation();
          setModalIsOpen((isOpen) => !isOpen);
        }}
        {...buttonProps}
      >
        {buttonProps.children}
      </Button>
      {modalIsOpen && (
        <ModalBody {...modalProps} onClose={() => setModalIsOpen(false)}>
          {children}
        </ModalBody>
      )}
    </Root>
  );
};
