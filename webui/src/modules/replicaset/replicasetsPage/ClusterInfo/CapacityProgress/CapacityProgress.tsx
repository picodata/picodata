import React from "react";
import cn from "classnames";

import { formatBytes } from "shared/utils/format/formatBytes";

import { CapacityProgressLine } from "./CapacityProgressLine/CapacityProgressLine";

import styles from "./CapacityProgress.module.scss";

type CapacityProgressProps = {
  currentValue: number;
  limit: number;
  percent?: number;
  theme?: "primary" | "secondary";
  size?: "small" | "medium";
  currentValueLabel?: string;
  progressLineWidth?: number | string;
};

export const CapacityProgress: React.FC<CapacityProgressProps> = (props) => {
  const {
    size = "medium",
    theme = "primary",
    currentValue,
    limit,
    currentValueLabel = "",
    progressLineWidth = 261,
  } = props;

  const percent = Math.round(props.percent ?? (currentValue / limit) * 100);

  const showLabels = !!currentValueLabel;

  return (
    <div className={cn(styles.container, styles[size], styles[theme])}>
      <div className={styles.text}>{percent} %</div>
      <div className={styles.progressLineContainer}>
        <CapacityProgressLine
          width={progressLineWidth}
          percent={percent}
          theme={theme}
          size={size}
        />
        <div className={styles.progressLineInfo}>
          <div className={styles.text}>{formatBytes(currentValue)}</div>
          <div className={styles.text}>{formatBytes(limit)}</div>
        </div>
        {showLabels && (
          <div className={styles.progressLabels}>
            <div className={styles.label}>{currentValueLabel}</div>
          </div>
        )}
      </div>
    </div>
  );
};
