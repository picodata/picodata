import React from "react";
import cn from "classnames";

import styles from "./Option.module.scss";

type OptionProps = {
  isSelected: boolean;
  children: React.ReactNode;
} & React.ButtonHTMLAttributes<HTMLDivElement>;

export const Option: React.FC<OptionProps> = (props) => {
  const { isSelected, children, ...other } = props;

  return (
    <div
      className={cn(styles.item, isSelected && styles.selectedItem)}
      {...other}
    >
      {children}
    </div>
  );
};
