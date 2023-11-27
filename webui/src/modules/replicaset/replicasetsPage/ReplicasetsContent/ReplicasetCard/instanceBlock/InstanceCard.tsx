import cn from "classnames";
import React, { FC, useCallback, useState } from "react";

import { LeaderIcon } from "shared/icons/LeaderIcon";
import { ClientInstanceType } from "store/slices/types";
import { formatFailDomains } from "modules/replicaset/replicasetsPage/ReplicasetsContent/utils";

import { InstanceModal } from "./instanceModal/InstanceModal";

import styles from "./InstanceCard.module.scss";

interface InstanceCardProps {
  instance: ClientInstanceType;
  theme?: "primary" | "secondary";
}

export const InstanceCard: FC<InstanceCardProps> = React.memo(
  ({ instance, theme = "primary" }) => {
    const [isOpenModal, setIsOpenModal] = useState<boolean>(false);
    const onCloseHandler = useCallback(() => {
      setIsOpenModal(false);
    }, []);

    return (
      <>
        <div
          onClick={(event) => {
            event.stopPropagation();
            setIsOpenModal(true);
          }}
          className={cn(styles.instanceWrapper, styles[theme])}
        >
          <div className={styles.infoColumn}>
            <div className={styles.instanceNameLabel}>
              <div className={styles.label}>Instance name</div>
              {instance.isLeader && (
                <LeaderIcon className={styles.leaderIcon} />
              )}
            </div>
            <div className={styles.value}>{instance.name}</div>
          </div>
          <div className={cn(styles.infoColumn, styles.targetGradeColumn)}>
            <div className={styles.label}>Target grade</div>
            <div className={styles.value}>{instance.targetGrade}</div>
          </div>
          <div className={cn(styles.infoColumn, styles.currentGradeColumn)}>
            <div className={styles.label}>Current grade</div>
            <div className={styles.value}>{instance.currentGrade}</div>
          </div>
          <div className={cn(styles.infoColumn, styles.domainColumn)}>
            <div className={styles.label}>Failure domain</div>
            <div className={styles.value}>
              {formatFailDomains(instance.failureDomain)}
            </div>
          </div>
          <div className={cn(styles.infoColumn, styles.versionColumn)}>
            <div className={styles.label}>Version</div>
            <div className={styles.value}>{instance.version}</div>
          </div>
        </div>
        <InstanceModal
          key={`${instance.name}_modal`}
          instance={instance}
          isOpen={isOpenModal}
          onClose={onCloseHandler}
        />
      </>
    );
  }
);
