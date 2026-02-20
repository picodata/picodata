import { SxProps } from "@mui/material";

import { Content } from "shared/ui/layout/Content/Content";
import { useTranslation } from "shared/intl";
import { useClusterInfo } from "shared/entity/cluster/info";

import { CapacityProgress } from "./CapacityProgress/CapacityProgress";
import {
  CapacityInfoColumn,
  CapacityWrapper,
  ColumnContent,
  ColumnLabel,
  ColumnName,
  ColumnValue,
  containerSx,
  InstancesBlock,
  RightColumn,
  RightContainer,
} from "./StyledComponents";

type ClusterInfoProps = {
  className?: string;
  sx?: SxProps;
};

export const ClusterInfo = ({ sx }: ClusterInfoProps) => {
  const { data: clusterInfoData } = useClusterInfo();

  const { translation } = useTranslation();
  const clusterTranslations = translation.pages.instances.cluster;

  if (!clusterInfoData) {
    return null;
  }
  const instancesCurrentStateOfflineIsPositive =
    clusterInfoData.instancesCurrentStateOffline > 0;
  return (
    <Content
      sx={
        {
          ...sx,
          ...containerSx,
        } as SxProps
      }
    >
      <CapacityInfoColumn>
        <ColumnName>{clusterTranslations.capacityProgress.label}</ColumnName>
        <CapacityWrapper>
          <CapacityProgress
            percent={clusterInfoData.capacityUsage}
            currentValue={clusterInfoData.memory.used}
            limit={clusterInfoData.memory.usable}
            currentValueLabel={clusterTranslations.capacityProgress.valueLabel}
          />
        </CapacityWrapper>
      </CapacityInfoColumn>
      <RightContainer>
        <RightColumn>
          <ColumnName>{clusterTranslations.plugins.label}</ColumnName>
          <ColumnContent>
            <ColumnValue>
              {(clusterInfoData.plugins as Array<unknown>).map((plugin, i) =>
                i == 0 ? (
                  <>{plugin}</>
                ) : (
                  <>
                    <br />
                    {plugin}
                  </>
                )
              )}
            </ColumnValue>
          </ColumnContent>
        </RightColumn>
        <RightColumn>
          <ColumnName>{clusterTranslations.replicasets.label}</ColumnName>
          <ColumnContent>
            <ColumnValue>{clusterInfoData.replicasetsCount}</ColumnValue>
            <ColumnLabel>
              {clusterTranslations.replicasets.description}
            </ColumnLabel>
          </ColumnContent>
        </RightColumn>
        <RightColumn>
          <ColumnName>{clusterTranslations.instances.label}</ColumnName>
          <InstancesBlock>
            <ColumnContent>
              <ColumnValue>
                {clusterInfoData.instancesCurrentStateOnline}
              </ColumnValue>
              <ColumnLabel>
                {clusterTranslations.instances.onlineState}
              </ColumnLabel>
            </ColumnContent>
            <ColumnContent>
              <ColumnValue $isWarning={instancesCurrentStateOfflineIsPositive}>
                {clusterInfoData.instancesCurrentStateOffline}
              </ColumnValue>
              <ColumnLabel $isWarning={instancesCurrentStateOfflineIsPositive}>
                {clusterTranslations.instances.offlineState}
              </ColumnLabel>
            </ColumnContent>
          </InstancesBlock>
        </RightColumn>
        <RightColumn>
          <ColumnName>{clusterTranslations.version.label}</ColumnName>
          <ColumnContent>
            <ColumnValue>
              {(clusterInfoData as unknown).currentInstaceVersion}
            </ColumnValue>
            <ColumnLabel>{clusterTranslations.version.description}</ColumnLabel>
          </ColumnContent>
        </RightColumn>
      </RightContainer>
    </Content>
  );
};
