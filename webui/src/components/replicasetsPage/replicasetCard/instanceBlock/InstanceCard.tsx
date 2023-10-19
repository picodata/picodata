import cn from "classnames";
import { FC, useCallback, useState } from "react";
import { InstanceModal } from "./instanceModal/InstanceModal";
import { LeaderIcon } from "components/icons/LeaderIcon";

import styles from "./InstanceCard.module.css";
import { ClientInstanceType } from "store/slices/types";
import { formatFailDomains } from "components/replicasetsPage/itemsGrid/utils";

interface InstanceCardProps {
  instance: ClientInstanceType;
  theme?: "primary" | "secondary";
}

export const InstanceCard: FC<InstanceCardProps> = ({
  instance,
  theme = "primary",
}) => {
  const [isOpenModal, setIsOpenModal] = useState<boolean>(false);
  const onCloseHandler = useCallback(() => {
    setIsOpenModal(false);
  }, []);

  return (
    <>
      <div
        onClick={() => setIsOpenModal(true)}
        className={cn(styles.instanceWrapper, styles[theme])}
      >
        <div className={styles.infoColumn}>
          <div className={styles.instanceNameLabel}>
            <p className={styles.noMargin}>Instance name</p>
            {instance.isLeader && <LeaderIcon className={styles.leaderIcon} />}
          </div>
          <div className={styles.instanceNameBlock}>
            <p className={styles.instanceInfo}>{instance.name}</p>
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
          <p className={styles.instanceInfo}>
            {formatFailDomains(instance.failureDomain)}
          </p>
        </div>
        <div className={styles.infoColumn}>
          <p className={styles.noMargin}>Version</p>
          <p className={styles.instanceInfo}>{instance.version}</p>
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
};
