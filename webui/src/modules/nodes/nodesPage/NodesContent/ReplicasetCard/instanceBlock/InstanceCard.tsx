import cn from "classnames";
import React, { FC } from "react";

import { LeaderIcon } from "shared/icons/LeaderIcon";
import { InstanceType } from "shared/entity/instance";
import { TextInFrame } from "shared/ui/typography/TextInFrame/TextInFrame";

import { FailureDomainLabel } from "./FailureDomainLabel/FailureDomainLabel";
import { IpAddressLabel } from "./IpAddressLabel/IpAddressLabel";

import styles from "./InstanceCard.module.scss";

interface InstanceCardProps {
  classes?: { cardWrapper?: string };
  instance: InstanceType;
  theme?: "primary" | "secondary";
}

export const InstanceCard: FC<InstanceCardProps> = React.memo(
  ({ instance, theme = "primary", classes }) => {
    return (
      <>
        <div
          onClick={(event) => {
            event.stopPropagation();
          }}
          className={cn(
            styles.cardWrapper,
            styles[theme],
            classes?.cardWrapper
          )}
        >
          <div className={styles.content}>
            <div className={cn(styles.infoColumn, styles.nameColumn)}>
              <div className={styles.label}>Instance name</div>
              <div className={cn(styles.value, styles.nameValue)}>
                {instance.name}
                {instance.isLeader && <LeaderIcon />}
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.failureDomainColumn)}>
              <div className={styles.label}>Failure domain</div>
              <div className={cn(styles.value, styles.domainValue)}>
                <FailureDomainLabel failureDomain={instance.failureDomain} />
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.targetGradeColumn)}>
              <div className={styles.label}>Target grade</div>
              <div className={cn(styles.value, styles.targetGradeValue)}>
                <TextInFrame>{instance.targetGrade}</TextInFrame>
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.currentGradeColumn)}>
              <div className={styles.label}>Current grade</div>
              <div className={cn(styles.value, styles.currentGradeValue)}>
                <TextInFrame>{instance.currentGrade}</TextInFrame>
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.binaryAddressColumn)}>
              <div className={styles.label}>Binary address</div>
              <IpAddressLabel
                className={styles.value}
                address={instance.binaryAddress}
              />
            </div>
            <div className={cn(styles.infoColumn)}>
              <div className={styles.label}>HTTP address</div>
              <IpAddressLabel
                className={styles.value}
                address={instance.httpAddress ?? ""}
              />
            </div>
            <div className={cn(styles.infoColumn, styles.versionColumn)}>
              <div className={styles.label}>Version</div>
              <div className={styles.value}>{instance.version}</div>
            </div>
          </div>
        </div>
        {/* <InstanceModal
          key={`${instance.name}_modal`}
          instance={instance}
          isOpen={isOpenModal}
          onClose={onCloseHandler}
        /> */}
      </>
    );
  }
);
