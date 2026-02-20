import { FC } from "react";

import { Spinner } from "../Spinner";

import { LoadingType, LoadingIndicatorProps } from "./LoadingIndicator.types";
import { Root } from "./StyledComponents";

export const LoadingIndicator: FC<LoadingIndicatorProps> = ({
  size = 48,
  type = LoadingType.ABSOLUTE,
}) => {
  return (
    <Root
      style={{
        position: type,
      }}
    >
      <Spinner size={size} />
    </Root>
  );
};
