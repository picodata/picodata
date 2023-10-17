import { FC, useMemo } from "react";
import { createPortal } from "react-dom";
import styles from "./InstanceModal.module.css";
import { CloseIcon } from "components/icons/CloseIcon";
import { LeaderBigIcon } from "components/icons/LeaderBigIcon";
import classNames from "classnames";
import { ClientInstanceType } from "store/slices/types";

export interface InstanceModalProps {
  isOpen: boolean;
  onClose: () => void;
  instance: ClientInstanceType;
}

export const InstanceModal: FC<InstanceModalProps> = ({
  isOpen,
  onClose,
  instance,
}) => {
  const boxInfoEl = useMemo(() => {
    const keys = Object.keys(instance);
    return (
      <div className={styles.boxInfoWrapper}>
        {keys.map((key, index) => {
          if (typeof instance[key as keyof ClientInstanceType] === "string") {
            return (
              <div
                className={classNames(styles.boxInfoRaw, {
                  [styles.grayRaw]: index % 2 !== 0,
                })}
              >
                <span>
                  <p className={styles.titleText}>{key}</p>
                </span>
                <p>{instance[key as keyof ClientInstanceType].toString()}</p>
              </div>
            );
          }
        })}
      </div>
    );
  }, [instance]);

  if (!isOpen) {
    return null;
  }

  return createPortal(
    <div className={styles.wrapper}>
      <div className={styles.body}>
        <div className={styles.titleWrapper}>
          <span className={styles.titleText}>
            {instance.isLeader && <LeaderBigIcon />}
            {instance.name}
          </span>
          <CloseIcon onClick={onClose} />
        </div>
        <div className={styles.tabGroup}>
          <span>General</span>
        </div>
        <div className={styles.boxInfo}>{boxInfoEl}</div>
      </div>
    </div>,
    document.body
  );
};
