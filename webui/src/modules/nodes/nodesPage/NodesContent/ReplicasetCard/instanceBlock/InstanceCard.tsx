import cn from "classnames";
import React, { FC, ReactNode } from "react";

import { InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import { FailureDomainLabel } from "./FailureDomainLabel/FailureDomainLabel";
import { AddressBlock } from "./AddressBlock/AddressBlock";

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
          {instance.isLeader ? (
            <div className={styles.leaderBlock}>
              {instanceTranslations.leader.label}
            </div>
          ) : (
            <div className={styles.followerBlock} />
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
            <div className={styles.joinedColumn}>
              <div className={cn(styles.infoColumn, styles.currentStateColumn)}>
                <div className={styles.label}>
                  {instanceTranslations.currentState.label}
                </div>
                <div className={cn(styles.value, styles.currentStateValue)}>
                  <NetworkState state={instance.currentState} />
                </div>
              </div>
            </div>
            <AddressBlock
              addresses={[
                {
                  title: instanceTranslations.binaryAddress.label,
                  value: instance.binaryAddress,
                },
                {
                  title: instanceTranslations.httpAddress.label,
                  value: instance.httpAddress ?? "",
                },
                {
                  title: instanceTranslations.pgAddress.label,
                  value: instance.pgAddress,
                },
              ]}
            />
            <VersionBlock
              className={cn(styles.infoColumn, styles.hiddenValue)}
              label={instanceTranslations.version.label}
              version={instance.version}
              noData={
                <InfoNoData text={translation.components.infoNoData.label} />
              }
            />
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

function VersionBlock(props: {
  label: string;
  version: string;
  noData?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn(props.className, styles.versionColumn)}>
      <div className={styles.label}>{props.label}</div>
      {props.version ? (
        <div className={cn(styles.value, styles.hiddenValue)}>
          <HiddenWrapper>{props.version}</HiddenWrapper>
        </div>
      ) : (
        props.noData
      )}
    </div>
  );
}
