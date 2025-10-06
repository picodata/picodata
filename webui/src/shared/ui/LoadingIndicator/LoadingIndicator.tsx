import { FC } from "react";

import { Spinner } from "../Spinner";

import { LoadingType, LoadingIndicatorProps } from "./LoadingIndicator.types";

import styles from "./LoadingIndicator.module.scss";

export const LoadingIndicator: FC<LoadingIndicatorProps> = ({
  size = 48,
  type = LoadingType.ABSOLUTE,
}) => {
  return (
    <div
      className={styles.loadingIndicator}
      style={{
        position: type,
      }}
    >
      <Spinner size={size} />
    </div>
  );
};
