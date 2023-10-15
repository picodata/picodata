import React from "react";
import cn from "classnames";

import styles from "./FilterKey.module.scss";

type FilterKeyProps = {
  children: React.ReactNode;
  isActive: boolean;
} & React.ButtonHTMLAttributes<HTMLDivElement>;

export const FilterKey: React.FC<FilterKeyProps> = (props) => {
  const { children, isActive, ...other } = props;

  return (
    <div
      className={cn(styles.container, isActive && styles.activeContainer)}
      {...other}
    >
      {children}
    </div>
  );
};
