import { FC, useMemo } from "react";
import { createPortal } from "react-dom";
import classNames from "classnames";

import { CloseIcon } from "shared/icons/CloseIcon";
import { LeaderBigIcon } from "shared/icons/LeaderBigIcon";
import { InstanceType } from "shared/entity/instance";

import styles from "./InstanceModal.module.css";

export interface InstanceModalProps {
  isOpen: boolean;
  onClose: () => void;
  instance: InstanceType;
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
          if (typeof instance[key as keyof InstanceType] === "string") {
            return (
              <div
                key={index}
                className={classNames(styles.boxInfoRaw, {
                  [styles.grayRaw]: index % 2 !== 0,
                })}
              >
                <span>
                  <p className={styles.titleText}>{key}</p>
                </span>
                <p>{instance[key as keyof InstanceType]?.toString()}</p>
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
    <div
      className={styles.wrapper}
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
      }}
    >
      <div className={styles.body}>
        <div className={styles.titleWrapper}>
          <span className={styles.titleText}>
            {instance.isLeader && <LeaderBigIcon className={styles.starIcon} />}
            {instance.name}
          </span>
          <div
            className={styles.close}
            onClick={(event) => {
              event.stopPropagation();
              onClose();
            }}
          >
            <CloseIcon />
          </div>
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
