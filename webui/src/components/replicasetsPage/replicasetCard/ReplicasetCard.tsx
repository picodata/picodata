import styles from "./ReplicasetCard.module.css";
import classNames from "classnames";
import { Instance, InstanceBlock } from "./instanceBlock/InstanceBlock";
import { FC } from "react";

const cn = classNames;
export interface Replicaset {
  id: string;
  roles: string[];
  isProblem: boolean;
  status: string;
  instances: Instance[];
  errorsCount: number;
  version: string;
}
export interface ReplicasetCardProps {
  replicaset: Replicaset;
}
export const ReplicasetCard: FC<ReplicasetCardProps> = ({ replicaset }) => {
  return (
    <div className={styles.cardWrapper}>
      <div className={styles.topBlock}>
        <div className={styles.topLeftBlock}>
          <p className={styles.noMargin}>{replicaset.id}</p>
          <div className={styles.flexWrapper}>
            Replicaset roles :
            {replicaset.roles.map((role) => (
              <p className={cn(styles.noMargin, styles.role)}>{`${role} |`}</p>
            ))}
          </div>
        </div>
        <div
          className={cn(styles.topRightBlock, styles.good, {
            [styles.problem]: replicaset.isProblem,
          })}
        >
          {replicaset.status}
        </div>
      </div>
      <InstanceBlock instances={replicaset.instances} />
      <div className={styles.bottomBlock}>
        <p className={styles.noMargin}>{replicaset.version}</p>
        <p className={styles.noMargin}>Errors : {replicaset.errorsCount}</p>
      </div>
    </div>
  );
};
