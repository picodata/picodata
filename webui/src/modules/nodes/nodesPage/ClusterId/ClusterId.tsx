import { useMemo } from "react";

import { useClusterInfo } from "shared/entity/cluster/info";

import { Prefix, Root } from "./StyledComponents";

export const ClusterId = () => {
  const { data: clusterInfoData } = useClusterInfo();
  const clusterName = useMemo(
    () => clusterInfoData?.clusterName,
    [clusterInfoData]
  );

  return (
    <Root>
      {clusterName ? <span>{clusterName}</span> : <Prefix>Cluster ID</Prefix>}
    </Root>
  );
};
