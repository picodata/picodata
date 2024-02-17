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
      <div className={cn(styles.infoColumn, styles.capacityInfoColumn)}>
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
      <div className={cn(styles.infoColumn, styles.replicasetsColumn)}>
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
      <div className={cn(styles.infoColumn, styles.instancesColumn)}>
        <div className={styles.columnName}>
          {clusterTranslations.instances.label}
        </div>
        <div className={styles.instancesBlock}>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.instancesCurrentGradeOnline}
            </div>
            <div className={styles.columnLabel}>
              {clusterTranslations.instances.onlineGrade}
            </div>
          </div>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.instancesCurrentGradeOffline}
            </div>
            <div className={styles.columnLabel}>
              {clusterTranslations.instances.offlineGrade}
            </div>
          </div>
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.versionColumn)}>
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
    </Content>
  );
};
