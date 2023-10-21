import { FC, useState } from "react";
import cn from "classnames";

import { ChevronDown } from "shared/icons/ChevronDown";
import { ClientInstanceType } from "store/slices/types";

import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";

import { InstanceCard } from "./instanceBlock/InstanceCard";

import styles from "./ReplicasetCard.module.scss";

export type TReplicaset = {
  id: string;
  instanceCount: number;
  instances: ClientInstanceType[];
  version: string;
  grade: string;
  capacity: number;
};
export interface ReplicasetCardProps {
  replicaset: TReplicaset;
}

/** Узнать откуда взять значения для прогресса ...*/
export const ReplicasetCard: FC<ReplicasetCardProps> = ({ replicaset }) => {
  const [isOpen, setIsOpen] = useState<boolean>(false);

  return (
    <div className={styles.cardWrapper} onClick={() => setIsOpen(!isOpen)}>
      <div className={styles.replicasetInfo}>
        <div className={cn(styles.infoColumn, styles.nameColumn)}>
          <div className={styles.label}>Name</div>
          <div className={styles.infoValue}>{replicaset.id}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>Instances</div>
          <div className={styles.infoValue}>{replicaset.instanceCount}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>Grade</div>
          <div className={styles.infoValue}>{replicaset.grade}</div>
        </div>
        <div className={cn(styles.infoColumn, styles.capacityColumn)}>
          <div className={styles.label}>Capacity</div>
          <CapacityProgress
            percent={replicaset.capacity}
            currentValue={1200000}
            limit={1200000}
            size="small"
            theme="secondary"
            progressLineWidth={190}
          />
        </div>
        <div className={cn(styles.infoColumn, styles.chevronColumn)}>
          <ChevronDown
            className={cn(styles.chevronIcon, isOpen && styles.chevronIconOpen)}
          />
        </div>
      </div>
      {isOpen && (
        <div className={styles.instancesWrapper}>
          {replicaset.instances.map((instance) => (
            <InstanceCard key={instance.name} instance={instance} />
          ))}
        </div>
      )}
    </div>
  );
};
