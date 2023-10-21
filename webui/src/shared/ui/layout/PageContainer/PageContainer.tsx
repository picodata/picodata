import React from "react";
import cn from "classnames";

import styles from "./PageContainer.module.scss";

type PageContainerProps = React.HTMLAttributes<HTMLDivElement>;

export const PageContainer: React.FC<PageContainerProps> = (props) => {
  const { className, ...other } = props;

  return <div className={cn(styles.container, className)} {...other}></div>;
};
