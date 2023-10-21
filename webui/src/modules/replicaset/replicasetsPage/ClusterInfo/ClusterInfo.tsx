import { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import cn from "classnames";

import { AppDispatch, RootState } from "store";
import { getClusterInfo } from "store/slices/clusterSlice";
import { Content } from "shared/ui/layout/Content/Content";

import { CapacityProgress } from "./CapacityProgress/CapacityProgress";

import styles from "./ClusterInfo.module.scss";

type ClusterInfoProps = {
  className?: string;
};

export const ClusterInfo = (props: ClusterInfoProps) => {
  const { className } = props;

  const dispatch = useDispatch<AppDispatch>();
  const clusterInfoSelector = useSelector(
    (state: RootState) => state.cluster.clusterInfo
  );
  useEffect(() => {
    dispatch(getClusterInfo());
  }, [dispatch]);

  return (
    <Content className={cn(styles.container, className)}>
      <div className={cn(styles.infoColumn, styles.capacityInfoColumn)}>
        <div className={styles.columnName}>Capacity Usage</div>
        <div className={styles.capacityWrapper}>
          <CapacityProgress
            percent={clusterInfoSelector.capacityUsage}
            currentValue={clusterInfoSelector.memory.used}
            limit={clusterInfoSelector.memory.usable}
            currentValueLabel="Useful capacity"
          />
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.replicasetsColumn)}>
        <div className={styles.columnName}>Replicasets</div>
        <div className={styles.columnContent}>
          <div className={styles.columnValue}>
            {clusterInfoSelector.replicasetsCount}
          </div>
          <div className={styles.columnLabel}>
            total <br />
            replicasets
          </div>
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.instancesColumn)}>
        <div className={styles.columnName}>Instances</div>
        <div className={styles.instancesBlock}>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoSelector.instancesCurrentGradeOnline}
            </div>
            <div className={styles.columnLabel}>
              current grade <br />
              online
            </div>
          </div>
          <div className={styles.columnContent}>
            <div className={styles.columnValue}>
              {clusterInfoSelector.instancesCurrentGradeOffline}
            </div>
            <div className={styles.columnLabel}>
              current grade <br />
              offline
            </div>
          </div>
        </div>
      </div>
      <div className={cn(styles.infoColumn, styles.versionColumn)}>
        <div className={styles.columnName}>Version</div>
        <div className={styles.columnContent}>
          <div className={styles.columnValue}>
            {clusterInfoSelector.currentInstaceVersion}
          </div>
          <div className={styles.columnLabel}>
            current <br /> instance
          </div>
        </div>
      </div>
    </Content>
  );
};
