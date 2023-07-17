import { useCallback } from "react";
import styles from "./InstanceBlock.module.css";

export const InstanceBlock = ({ instances }) => {
  const instanceEl = useCallback(
    (instance) => (
      <div className={styles.instanceWrapper}>
        <p className={styles.noMargin}>{instance.name}</p>
        <p className={styles.noMargin}>{instance.info}</p>
      </div>
    ),
    []
  );

  return (
    <div className={styles.instancesWrapper}>
      {instances.length > 0 &&
        instances.map((instance) => instanceEl(instance))}
    </div>
  );
};
