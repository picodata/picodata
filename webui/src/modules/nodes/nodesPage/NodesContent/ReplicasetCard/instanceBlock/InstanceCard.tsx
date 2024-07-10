import cn from "classnames";
import React, { FC } from "react";

import { InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

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
    const { translation } = useTranslation();
    const instanceTranslations = translation.pages.instances.list.instanceCard;

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
          {instance.isLeader && (
            <div className={styles.leaderBlock}>
              {instanceTranslations.leader.label}
            </div>
          )}
          <div className={styles.content}>
            <div
              className={cn(
                styles.infoColumn,
                styles.nameColumn,
                styles.hiddenColumn
              )}
            >
              {theme === "primary" && (
                <div className={styles.label}>
                  {instanceTranslations.name.label}
                </div>
              )}
              <div
                className={cn(
                  styles.value,
                  styles.hiddenValue,
                  styles.startValue
                )}
              >
                <HiddenWrapper>{instance.name}</HiddenWrapper>
              </div>
            </div>
            <div
              className={cn(
                styles.infoColumn,
                styles.failureDomainColumn,
                styles.hiddenColumn
              )}
            >
              <div className={styles.label}>
                {instanceTranslations.failureDomain.label}
              </div>
              <div className={cn(styles.value, styles.domainValue)}>
                <FailureDomainLabel failureDomain={instance.failureDomain} />
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.targetGradeColumn)}>
              <div className={styles.label}>
                {instanceTranslations.targetGrade.label}
              </div>
              <div className={cn(styles.value, styles.targetGradeValue)}>
                <NetworkState state={instance.targetGrade} />
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.currentGradeColumn)}>
              <div className={styles.label}>
                {instanceTranslations.currentGrade.label}
              </div>
              <div className={cn(styles.value, styles.currentGradeValue)}>
                <NetworkState state={instance.currentGrade} />
              </div>
            </div>
            <div className={cn(styles.infoColumn, styles.binaryAddressColumn)}>
              <div className={styles.label}>
                {instanceTranslations.binaryAddress.label}
              </div>
              <IpAddressLabel
                className={cn(styles.value, styles.hiddenValue)}
                address={instance.binaryAddress}
              />
            </div>
            <div className={cn(styles.infoColumn, styles.httpAddressColumn)}>
              <div className={styles.label}>
                {instanceTranslations.httpAddress.label}
              </div>
              <IpAddressLabel
                className={cn(styles.value, styles.hiddenValue)}
                address={instance.httpAddress ?? ""}
              />
            </div>
            <div
              className={cn(
                styles.infoColumn,
                styles.versionColumn,
                styles.hiddenValue
              )}
            >
              <div className={styles.label}>
                {instanceTranslations.version.label}
              </div>
              <div className={cn(styles.value, styles.hiddenValue)}>
                <HiddenWrapper>{instance.version}</HiddenWrapper>
              </div>
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
