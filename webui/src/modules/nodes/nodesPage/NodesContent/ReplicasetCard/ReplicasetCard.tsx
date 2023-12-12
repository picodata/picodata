import React, { FC, useState } from "react";
import cn from "classnames";

import { ChevronDown } from "shared/icons/ChevronDown";
import { InstanceType } from "shared/entity/instance";
import { TextInFrame } from "shared/ui/typography/TextInFrame/TextInFrame";
import { Collapse } from "shared/ui/Collapse/Collapse";

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
  theme?: "primary" | "secondary";
  replicaset: TReplicaset;
}

export const ReplicasetCard: FC<ReplicasetCardProps> = React.memo(
  ({ replicaset, theme = "primary" }) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);

    const onClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
      event.stopPropagation();
      setIsOpen(!isOpen);
    };

    return (
      <div className={cn(styles.cardWrapper, styles[theme])} onClick={onClick}>
        <div className={styles.content}>
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
            <div className={cn(styles.infoValue, styles.gradeValue)}>
              <TextInFrame>{replicaset.grade}</TextInFrame>
            </div>
          </div>
          <div className={cn(styles.infoColumn, styles.capacityColumn)}>
            <CapacityProgress
              percent={replicaset.capacityUsage}
              currentValue={replicaset.memory.used}
              limit={replicaset.memory.usable}
              size="small"
              theme="secondary"
              progressLineWidth="100%"
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
        <Collapse isOpen={isOpen}>
          <div className={styles.instancesWrapper}>
            {replicaset.instances.map((instance) => (
              <InstanceCard
                key={instance.name}
                instance={instance}
                theme="secondary"
                classes={{ cardWrapper: styles.instancesCardWrapper }}
              />
            ))}
          </div>
        </Collapse>
      </div>
    );
  }
);
