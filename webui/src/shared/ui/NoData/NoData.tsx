import React from "react";
import cn from "classnames";

import styles from "./NoData.module.scss";

type NoDataProps = {
  children: React.ReactNode;
} & Omit<React.HTMLAttributes<HTMLDivElement>, "children">;

export const NoData: React.FC<NoDataProps> = (props) => {
  const { className, children, ...other } = props;

  return (
    <div className={cn(styles.container, className)} {...other}>
      {children}
    </div>
  );
};
