import React, { useRef, useState } from "react";
import cn from "classnames";

import { useOutsideClickEvent } from "components/shared/react/hooks/useOutsideClickEvent";
import { Button, ButtonProps } from "../Button/Button";
import { ModalBody, ModalBodyProps } from "../Modal/ModalBody/ModalBody";

import styles from "./ButtonModal.module.scss";

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
    <div className={styles.container} ref={containerRef}>
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
        <ModalBody
          {...modalProps}
          bodyClassName={cn(styles.modalBody, modalProps.bodyClassName)}
          onClose={() => setModalIsOpen(false)}
        >
          {children}
        </ModalBody>
      )}
    </div>
  );
};
