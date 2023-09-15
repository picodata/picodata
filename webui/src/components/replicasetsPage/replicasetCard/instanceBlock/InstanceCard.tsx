import { FC, useCallback, useState } from "react";
import { InstanceType } from "store/slices/types";
import styles from "./InstanceCard.module.css";
import { InstanceModal } from "./instanceModal/InstanceModal";
import { LeaderIcon } from "components/icons/LeaderIcon";

interface InstanceCardProps {
  instance: InstanceType;
}

export const InstanceCard: FC<InstanceCardProps> = ({ instance }) => {
  const [isOpenModal, setIsOpenModal] = useState<boolean>(false);
  const onCloseHandler = useCallback(() => {
    setIsOpenModal(false);
  }, []);

  return (
    <>
      <div
        onClick={() => setIsOpenModal(true)}
        className={styles.instanceWrapper}
      >
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
      <InstanceModal
        key={instance.name}
        instance={instance}
        isOpen={isOpenModal}
        onClose={onCloseHandler}
      />
    </>
  );
};
