import { PageContainer } from "shared/ui/layout/PageContainer/PageContainer";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { NodesContent } from "./NodesContent/NodesContent";
import { ClusterId } from "./ClusterId/ClusterId";
import { clusterInfoSx } from "./StyledComponents";

export const NodesPage = () => {
  return (
    <PageContainer>
      <ClusterId />
      <ClusterInfo sx={clusterInfoSx} />
      <NodesContent />
    </PageContainer>
  );
};
