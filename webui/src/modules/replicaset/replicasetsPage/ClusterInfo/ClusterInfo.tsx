import { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import cn from "classnames";

import { AppDispatch, RootState } from "store";
import { getClusterInfo } from "store/slices/clusterSlice";

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
    <div className={cn(styles.wrapper, className)}>
      <div className={cn(styles.infoColumn, styles.capacityInfoColumn)}>
        <p className={styles.columnName}>Capacity Usage</p>
        <div className={styles.capacityWrapper}>
          <div className={styles.flexWrapper}>
            <CapacityProgress
              percent={clusterInfoSelector.capacityUsage}
              currentValue={clusterInfoSelector.memory.used}
              limit={clusterInfoSelector.memory.usable}
              currentValueLabel="Useful capacity"
            />
          </div>
        </div>
      </div>
      <div className={styles.infoColumn}>
        <p className={styles.columnName}>Replicasets</p>
        <div className={styles.centredWrapper}>
          <p className={styles.boldText}>
            {clusterInfoSelector.replicasetsCount}
          </p>
          <p className={styles.noMargin}>total replicasets</p>
        </div>
      </div>
      <div className={styles.infoColumn}>
        <p className={styles.columnName}>Instances</p>
        <div className={styles.instancesBlock}>
          <div className={styles.centredWrapper}>
            <p className={styles.boldText}>
              {clusterInfoSelector.instancesCurrentGradeOnline}
            </p>
            <p className={styles.noMargin}>current grade online</p>
          </div>
          <div className={styles.centredWrapper}>
            <p className={styles.boldText}>
              {clusterInfoSelector.instancesCurrentGradeOffline}
            </p>
            <p className={styles.noMargin}>current grade offline</p>
          </div>
        </div>
      </div>
      <div className={styles.infoColumn}>
        <p className={styles.columnName}>Version</p>
        <div className={styles.centredWrapper}>
          <p className={styles.boldText}>
            {clusterInfoSelector.currentInstaceVersion}
          </p>
          <p className={styles.noMargin}>current instance</p>
        </div>
      </div>
    </div>
  );
};
