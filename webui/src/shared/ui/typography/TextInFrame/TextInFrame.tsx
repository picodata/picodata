import React from "react";
import cn from "classnames";

import styles from "./TextInFrame.module.scss";

export type TextInFrameProps = {
  className?: string;
  children: React.ReactNode;
};

export const TextInFrame: React.FC<TextInFrameProps> = (props) => {
  return (
    <div className={cn(styles.container, props.className)}>
      {props.children}
    </div>
  );
};
