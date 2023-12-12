import cn from "classnames";

import { Content } from "shared/ui/layout/Content/Content";

import { useClusterInfo } from "../../../../shared/entity/cluster/info";

import { CapacityProgress } from "./CapacityProgress/CapacityProgress";

import styles from "./ClusterInfo.module.scss";

type ClusterInfoProps = {
  className?: string;
};

export const ClusterInfo = (props: ClusterInfoProps) => {
  const { className } = props;

  const { data: clusterInfoData } = useClusterInfo();

  if (!clusterInfoData) {
    return null;
  }

  return (
    <Content className={cn(styles.container, className)}>
      <div className={cn(styles.infoColumn, styles.capacityInfoColumn)}>
        <div className={styles.columnName}>Capacity Usage</div>
        <div className={styles.capacityWrapper}>
          <CapacityProgress
            percent={clusterInfoData.capacityUsage}
            currentValue={clusterInfoData.memory.used}
            limit={clusterInfoData.memory.usable}
            currentValueLabel="Useful capacity"
          />
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.replicasetsColumn)}>
        <div className={styles.columnName}>Replicasets</div>
        <div className={styles.columnContent}>
          <div className={styles.columnValue}>
            {clusterInfoData.replicasetsCount}
          </div>
          <div className={styles.columnLabel}>total replicasets</div>
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.instancesColumn)}>
        <div className={styles.columnName}>Instances</div>
        <div className={styles.instancesBlock}>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.instancesCurrentGradeOnline}
            </div>
            <div className={styles.columnLabel}>current grade online</div>
          </div>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoData.instancesCurrentGradeOffline}
            </div>
            <div className={styles.columnLabel}>current grade offline</div>
          </div>
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.versionColumn)}>
        <div className={styles.columnName}>Version</div>
        <div className={styles.columnContent}>
          <div className={styles.columnValue}>
            {clusterInfoData.currentInstaceVersion}
          </div>
          <div className={styles.columnLabel}>current instance</div>
        </div>
      </div>
    </Content>
  );
};
