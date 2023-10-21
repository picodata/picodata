import { FC, useMemo, useState } from "react";

import { ChevronDown } from "shared/icons/ChevronDown";
import { ChevronUp } from "shared/icons/ChevronUp";
import { ClientInstanceType } from "store/slices/types";

import { InstanceCard } from "./instanceBlock/InstanceCard";
import styles from "./ReplicasetCard.module.css";

export interface Replicaset {
  id: string;
  instanceCount: number;
  instances: ClientInstanceType[];
  version: string;
  grade: string;
  capacity: string;
}
export interface ReplicasetCardProps {
  replicaset: Replicaset;
}
export const ReplicasetCard: FC<ReplicasetCardProps> = ({ replicaset }) => {
  const [isOpen, setIsOpen] = useState<boolean>(false);

  const elChevron = useMemo(() => {
    if (!isOpen) {
      return <ChevronDown onClick={() => setIsOpen(true)} />;
    }
    return <ChevronUp onClick={() => setIsOpen(false)} />;
  }, [isOpen, setIsOpen]);

  return (
    <div className={styles.cardWrapper}>
      <div className={styles.replicasetInfo}>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}> Name</p>
          <p className={styles.infoValue}>{replicaset.id}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}> Instances</p>
          <p className={styles.infoValue}>{replicaset.instanceCount}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}> Grade</p>
          <p className={styles.infoValue}>{replicaset.grade}</p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}> Capacity usage</p>
          <p className={styles.infoValue}>{`${replicaset.capacity}%`}</p>
        </div>
        <div className={styles.infoColumn}>{elChevron}</div>
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
