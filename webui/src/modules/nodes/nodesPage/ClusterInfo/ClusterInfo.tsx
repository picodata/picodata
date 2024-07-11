import cn from "classnames";

import { Content } from "shared/ui/layout/Content/Content";
import { useTranslation } from "shared/intl";

import { useClusterInfo } from "../../../../shared/entity/cluster/info";

import { CapacityProgress } from "./CapacityProgress/CapacityProgress";

import styles from "./ClusterInfo.module.scss";

type ClusterInfoProps = {
  className?: string;
};

export const ClusterInfo = (props: ClusterInfoProps) => {
  const { className } = props;

  const { data: clusterInfoData } = useClusterInfo();

  const { translation } = useTranslation();
  const clusterTranslations = translation.pages.instances.cluster;

  if (!clusterInfoData) {
    return null;
  }

  return (
    <Content className={cn(styles.container, className)}>
      <div className={cn(styles.left, styles.capacityInfoColumn)}>
        <div className={styles.columnName}>
          {clusterTranslations.capacityProgress.label}
        </div>
        <div className={styles.capacityWrapper}>
          <CapacityProgress
            percent={clusterInfoData.capacityUsage}
            currentValue={clusterInfoData.memory.used}
            limit={clusterInfoData.memory.usable}
            currentValueLabel={clusterTranslations.capacityProgress.valueLabel}
          />
        </div>
      </div>
      <div className={styles.right}>
        <div className={cn(styles.rightColumn)}>
          <div className={styles.columnName}>
            {clusterTranslations.replicasets.label}
          </div>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.replicasetsCount}
            </div>
            <div className={styles.columnLabel}>
              {clusterTranslations.replicasets.description}
            </div>
          </div>
        </div>
        <div className={cn(styles.rightColumn)}>
          <div className={styles.columnName}>
            {clusterTranslations.instances.label}
          </div>
          <div className={styles.instancesBlock}>
            <div className={styles.columnContent}>
              <div className={styles.columnValue}>
                {clusterInfoData.instancesCurrentStateOnline}
              </div>
              <div className={styles.columnLabel}>
                {clusterTranslations.instances.onlineState}
              </div>
            </div>
            <div className={styles.columnContent}>
              <div className={styles.columnValue}>
                {clusterInfoData.instancesCurrentStateOffline}
              </div>
              <div className={styles.columnLabel}>
                {clusterTranslations.instances.offlineState}
              </div>
            </div>
          </div>
        </div>
        <div className={cn(styles.rightColumn)}>
          <div className={styles.columnName}>
            {clusterTranslations.version.label}
          </div>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.currentInstaceVersion}
            </div>
            <div className={styles.columnLabel}>
              {clusterTranslations.version.description}
            </div>
          </div>
        </div>
      </div>
    </Content>
  );
};
