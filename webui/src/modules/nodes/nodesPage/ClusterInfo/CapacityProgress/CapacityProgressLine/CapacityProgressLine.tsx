import React from "react";

import {
  ProgressLine,
  ProgressLineProps,
} from "shared/ui/ProgressLine/ProgressLine";

import {
  getConfigBySize,
  getConfigByTheme,
  getStrokeColorByPercent,
} from "./config";

export type CapacityProgressLineProps = Omit<ProgressLineProps, "height"> & {
  height?: number | string;
  theme?: "primary" | "secondary";
  size?: "small" | "medium";
};

export const CapacityProgressLine: React.FC<CapacityProgressLineProps> = (
  props
) => {
  const {
    percent,
    strokeColor,
    theme = "primary",
    size = "medium",
    ...other
  } = props;

  const themeConfig = getConfigByTheme(theme);
  const sizeConfig = getConfigBySize(size);

  return (
    <ProgressLine
      percent={percent}
      strokeColor={strokeColor ?? getStrokeColorByPercent(percent)}
      {...themeConfig}
      {...sizeConfig}
      {...other}
    />
  );
};
