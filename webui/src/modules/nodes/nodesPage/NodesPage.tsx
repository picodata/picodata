import { useTiers } from "shared/entity/tier";
import { useMemory } from "shared/entity/memory";
import { useNodes } from "shared/entity/node";
import { Progress } from "shared/ui/Progress";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { NodesContent } from "./NodesContent/NodesContent";
import {
  clusterInfoSx,
  ContentContainer,
  NodesPageContainer,
} from "./StyledComponents";

export const NodesPage = () => {
  const { data: tiers, isLoading: tiersIsLoading } = useTiers();
  const { data: memory, isLoading: memoryIsLoading } = useMemory();
  const data = useNodes(tiers, memory);
  const isLoading = tiersIsLoading || memoryIsLoading;
  return !isLoading ? (
    <NodesPageContainer>
      <ClusterInfo sx={clusterInfoSx} />
      <ContentContainer>
        <NodesContent data={data} />
      </ContentContainer>
    </NodesPageContainer>
  ) : (
    <Progress />
  );
};
