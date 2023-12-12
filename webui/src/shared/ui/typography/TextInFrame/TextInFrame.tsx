import React from "react";

import styles from "./TextInFrame.module.scss";

export type TextInFrameProps = {
  children: React.ReactNode;
};

export const TextInFrame: React.FC<TextInFrameProps> = (props) => {
  return <div className={styles.container}>{props.children}</div>;
};
