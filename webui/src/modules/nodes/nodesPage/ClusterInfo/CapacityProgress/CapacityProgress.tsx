import React from "react";

import { formatBytes } from "shared/utils/format/formatBytes";

import { CapacityProgressLine } from "./CapacityProgressLine/CapacityProgressLine";
import {
  Content,
  Label,
  ProgressLineContainer,
  ProgressLineInfo,
  Root,
  Text,
} from "./StyledComponents";

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
    progressLineWidth = 194,
  } = props;

  const percent = Math.round(props.percent ?? (currentValue / limit) * 100);

  const showLabels = !!currentValueLabel;

  return (
    <Root>
      <Content>
        <Text $size={size} $theme={theme}>
          {percent} %
        </Text>
        <ProgressLineContainer $size={size}>
          <CapacityProgressLine
            width={progressLineWidth}
            percent={percent}
            theme={theme}
            size={size}
          />
          <ProgressLineInfo $size={size}>
            <Text $size={size} $theme={theme}>
              {formatBytes(currentValue)}
            </Text>
            <Text $size={size} $theme={theme}>
              {formatBytes(limit)}
            </Text>
          </ProgressLineInfo>
        </ProgressLineContainer>
      </Content>
      {showLabels && <Label>{currentValueLabel}</Label>}
    </Root>
  );
};
