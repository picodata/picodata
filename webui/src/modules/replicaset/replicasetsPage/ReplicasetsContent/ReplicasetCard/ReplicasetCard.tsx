import React, { FC, useState } from "react";
import cn from "classnames";

import { ChevronDown } from "shared/icons/ChevronDown";
import { InstanceType } from "shared/entity/instance";
import { TextInFrame } from "shared/ui/typography/TextInFrame/TextInFrame";

import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";

import { InstanceCard } from "./instanceBlock/InstanceCard";

import styles from "./ReplicasetCard.module.scss";

export type TReplicaset = {
  id: string;
  instanceCount: number;
  instances: InstanceType[];
  version: string;
  grade: string;
  capacityUsage: number;
  memory: {
    usable: number;
    used: number;
  };
};
export interface ReplicasetCardProps {
  replicaset: TReplicaset;
}

export const ReplicasetCard: FC<ReplicasetCardProps> = React.memo(
  ({ replicaset }) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);

    const onClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
      event.stopPropagation();
      setIsOpen(!isOpen);
    };

    return (
      <div className={styles.cardWrapper} onClick={onClick}>
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
            <div className={styles.infoValue}>
              <TextInFrame>{replicaset.grade}</TextInFrame>
            </div>
          </div>
          <div className={cn(styles.infoColumn, styles.capacityColumn)}>
            <div className={styles.label}>Capacity</div>
            <CapacityProgress
              percent={replicaset.capacityUsage}
              currentValue={replicaset.memory.used}
              limit={replicaset.memory.usable}
              size="small"
              theme="secondary"
              progressLineWidth={190}
            />
          </div>
          <div className={cn(styles.infoColumn, styles.chevronColumn)}>
            <ChevronDown
              className={cn(
                styles.chevronIcon,
                isOpen && styles.chevronIconOpen
              )}
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
  }
);
