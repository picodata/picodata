import React from "react";
import cn from "classnames";
import { createPortal } from "react-dom";

import styles from "./Modal.module.scss";

type ModalProps = {
  children: React.ReactElement;
  containerClassName?: string;
  bodyClassName?: string;
};

export const Modal: React.FC<ModalProps> = (props) => {
  const { containerClassName, bodyClassName, children } = props;

  return createPortal(
    <div
      className={cn(styles.wrapper, containerClassName)}
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
      }}
    >
      <div className={cn(styles.body, bodyClassName)}>{children}</div>
    </div>,
    document.body
  );
};
