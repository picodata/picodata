import React from "react";

import styles from "./ProgressLine.module.css";

export type ProgressLineProps = {
  width: number;
  height: number;
  percent: number;
  strokeColor?: string;
  trailColor?: string;
};

export const ProgressLine: React.FC<ProgressLineProps> = (props) => {
  const { trailColor = "#F9F5F2", strokeColor = "#6FFF9F" } = props;

  return (
    <div
      style={{
        width: props.width,
        height: props.height,
        backgroundColor: trailColor,
      }}
      className={styles.container}
    >
      <div
        style={{
          width: (props.width / 100) * props.percent,
          backgroundColor: strokeColor,
        }}
        className={styles.fill}
      />
    </div>
  );
};
