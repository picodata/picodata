import { PageContainer } from "shared/ui/layout/PageContainer/PageContainer";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { NodesContent } from "./NodesContent/NodesContent";
import { ClusterId } from "./ClusterId/ClusterId";

import styles from "./NodesPage.module.scss";

export const NodesPage = () => {
  return (
    <PageContainer>
      <ClusterId />
      <ClusterInfo className={styles.clusterInfo} />
      <NodesContent />
    </PageContainer>
  );
};
