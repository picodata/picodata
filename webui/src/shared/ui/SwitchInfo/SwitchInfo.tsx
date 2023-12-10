import React from "react";
import cn from "classnames";

import styles from "./SwitchInfo.module.scss";

type SwitchProps = {
  checked: boolean;
};

export const SwitchInfo: React.FC<SwitchProps> = (props) => {
  const { checked } = props;

  return (
    <div
      className={cn(
        styles.container,
        checked ? styles.enabled : styles.disabled
      )}
    >
      <div className={styles.circle} />
    </div>
  );
};
