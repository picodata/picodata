import React from "react";
import cn from "classnames";

import styles from "./Content.module.scss";

type ContentProps = React.HTMLAttributes<HTMLDivElement>;

export const Content: React.FC<ContentProps> = (props) => {
  const { className, ...other } = props;

  return <div className={cn(styles.container, className)} {...other}></div>;
};
