import { PageContainer } from "shared/ui/layout/PageContainer/PageContainer";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { NodesContent } from "./NodesContent/NodesContent";

import styles from "./NodesPage.module.scss";

export const NodesPage = () => {
  return (
    <PageContainer>
      <div className={styles.title}>Cluster ID</div>
      <ClusterInfo className={styles.clusterInfo} />
      <NodesContent />
    </PageContainer>
  );
};
