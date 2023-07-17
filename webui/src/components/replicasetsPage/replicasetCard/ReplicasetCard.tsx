import styles from "./ReplicasetCard.module.css";
import classNames from "classnames";
import { InstanceBlock } from "./instanceBlock/InstanceBlock";

const cn = classNames;

export const ReplicasetCard = ({ replicaset }) => {
  return (
    <div className={styles.cardWrapper}>
      <div className={styles.topBlock}>
        <div className={styles.topLeftBlock}>
          <p className={styles.noMargin}>{replicaset.id}</p>
          <div className={styles.flexWrapper}>
            Replicaset roles :
            {replicaset.roles.map((role: string) => (
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
