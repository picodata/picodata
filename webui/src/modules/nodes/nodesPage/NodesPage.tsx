import { CircularProgress } from "@mui/material";

import { useTiers } from "../../../shared/entity/tier";

import { ClusterInfo } from "./ClusterInfo/ClusterInfo";
import { NodesContent } from "./NodesContent/NodesContent";
import {
  clusterInfoSx,
  ContentContainer,
  LoadContainer,
  NodesPageContainer,
} from "./StyledComponents";

export const NodesPage = () => {
  const { data, isLoading } = useTiers();
  return !isLoading ? (
    <NodesPageContainer>
      <ClusterInfo sx={clusterInfoSx} />
      <ContentContainer>
        <NodesContent data={data} />
      </ContentContainer>
    </NodesPageContainer>
  ) : (
    <LoadContainer>
      <CircularProgress />
    </LoadContainer>
  );
};
