import { useMemo } from "react";

import { useClusterInfo } from "shared/entity/cluster/info";

import styles from "./ClusterId.module.scss";

export const ClusterId = () => {
  const { data: clusterInfoData } = useClusterInfo();
  const clusterName = useMemo(
    () => clusterInfoData?.clusterName,
    [clusterInfoData]
  );

  return (
    <div className={styles.title}>
      {clusterName ? (
        <span>{clusterName}</span>
      ) : (
        <span className={styles.prefix}>Cluster ID</span>
      )}
    </div>
  );
};
