import React from "react";
import cn from "classnames";

import { CloseIcon } from "components/icons/CloseIcon";

import styles from "./ModalBody.module.scss";

export type ModalBodyProps = {
  title: React.ReactNode;
  children: React.ReactNode;
  bodyClassName?: string;
  onClose: () => void;
} & React.ButtonHTMLAttributes<HTMLDivElement>;

export const ModalBody: React.FC<ModalBodyProps> = (props) => {
  const { title, children, bodyClassName, onClose, ...other } = props;

  return (
    <div className={cn(styles.body, bodyClassName)} {...other}>
      <div className={styles.titleWrapper}>
        <span className={styles.titleText}>{title}</span>
        <CloseIcon onClick={onClose} className={styles.closeIcon} />
      </div>
      {children}
    </div>
  );
};
