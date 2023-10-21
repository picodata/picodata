import { PageContainer } from "shared/ui/layout/PageContainer/PageContainer";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { ReplecasetsContent } from "./ReplecasetsContent/ReplecasetsContent";

import styles from "./ReplicasetsPage.module.scss";

export const ReplicasetsPage = () => {
  return (
    <PageContainer>
      <ClusterInfo className={styles.clusterInfo} />
      <ReplecasetsContent />
    </PageContainer>
  );
};
