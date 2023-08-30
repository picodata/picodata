import { ClusterInfo } from "./clusterInfo/ClusterInfo";
import { ItemsGrid } from "./itemsGrid/ItemsGrid";
import styles from "./ReplicasetsPage.module.css";

export const ReplicasetsPage = () => {
  return (
    <div className={styles.pageWrapper}>
      <ClusterInfo />
      <ItemsGrid />
    </div>
  );
};
