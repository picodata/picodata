import { FC, useCallback } from "react";
import styles from "./InstanceBlock.module.css";
import { LeaderIcon } from "components/icons/LeaderIcon";

export interface Instance {
  name: string;
  targetGrade: string;
  currentGrade: string;
  failureDomain: string;
  version: string;
  isLeader: boolean;
}
export interface InstaceBlockProps {
  instances: Instance[];
}
export const InstanceBlock: FC<InstaceBlockProps> = ({ instances }) => {
  const instanceEl = useCallback(
    (instance: Instance) => (
      <div className={styles.instanceWrapper}>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Instance name</p>
          <div className={styles.instanceNameBlock}>
            <p className={styles.instanceInfo}>{instance.name}</p>
            {instance.isLeader && <LeaderIcon />}
          </div>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Target grade</p>
          <p className={styles.instanceInfo}>{instance.targetGrade}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Current grade</p>
          <p className={styles.instanceInfo}>{instance.currentGrade}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Failure domain</p>
          <p className={styles.instanceInfo}>{instance.failureDomain}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Version</p>
          <p className={styles.instanceInfo}>{instance.version}</p>
        </div>
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
