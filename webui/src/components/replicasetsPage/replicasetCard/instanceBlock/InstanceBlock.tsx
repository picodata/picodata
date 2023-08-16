import { FC, useCallback } from "react";
import styles from "./InstanceBlock.module.css";

export interface Instance {
  name: string;
  info: string;
}
export interface InstaceBlockProps {
  instances: Instance[];
}
export const InstanceBlock: FC<InstaceBlockProps> = ({ instances }) => {
  const instanceEl = useCallback(
    (instance: Instance) => (
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
