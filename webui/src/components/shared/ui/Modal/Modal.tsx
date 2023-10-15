import React from "react";
import cn from "classnames";

import styles from "./Modal.module.scss";
import { ModalBody, ModalBodyProps } from "./ModalBody/ModalBody";

type ModalProps = ModalBodyProps & {
  containerClassName?: string;
};

export const Modal: React.FC<ModalProps> = (props) => {
  const { containerClassName, ...bodyProps } = props;

  return (
    <div className={cn(styles.wrapper, containerClassName)}>
      <ModalBody {...bodyProps} />
    </div>
  );
};
