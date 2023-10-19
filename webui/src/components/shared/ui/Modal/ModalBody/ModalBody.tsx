import React from "react";
import cn from "classnames";

import { CloseIcon } from "components/icons/CloseIcon";

import styles from "./ModalBody.module.scss";

export type ModalBodyProps = {
  title: React.ReactNode;
  children: (args: {
    onClose: () => void;
  }) => Exclude<React.ReactNode, "string"> | React.ReactNode;
  bodyClassName?: string;
  onClose: () => void;
} & Omit<React.HtmlHTMLAttributes<HTMLDivElement>, "children">;

export const ModalBody: React.FC<ModalBodyProps> = (props) => {
  const { title, children, bodyClassName, onClose, ...other } = props;

  return (
    <div className={cn(styles.body, bodyClassName)} {...other}>
      <div className={styles.titleWrapper}>
        <span className={styles.titleText}>{title}</span>
        <CloseIcon onClick={onClose} className={styles.closeIcon} />
      </div>
      {typeof children === "function" ? children({ onClose }) : children}
    </div>
  );
};
