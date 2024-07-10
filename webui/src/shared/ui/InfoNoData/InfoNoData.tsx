import React from "react";
import cn from "classnames";

import styles from "./InfoNoData.module.scss";

type InfoNoDataProps = {
  text: string;
  className?: string;
};

export const InfoNoData: React.FC<InfoNoDataProps> = (props) => {
  const { text, className } = props;

  return <div className={cn(styles.text, className)}>{text}</div>;
};
