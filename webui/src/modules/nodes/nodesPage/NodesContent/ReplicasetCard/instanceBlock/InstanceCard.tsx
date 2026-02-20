import { memo, ReactNode } from "react";
import { Box, Tooltip } from "@mui/material";

import { InstanceNodeType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import {
  Cell,
  CellLabel,
  ContentFlexCell,
  ContentFlexCenteredCell,
  Ellipsis,
} from "../../common";

import { FailureDomainLabel } from "./FailureDomainLabel/FailureDomainLabel";
import { AddressBlock } from "./AddressBlock/AddressBlock";
import {
  ContentFlexCenteredNameCell,
  DomainValue,
  FollowerBlock,
  InstanceBackground,
  InstanceBackgroundInner,
  InstanceCell,
  InstanceItemRoot,
  InstanceNameCell,
  LeaderBlock,
  ValueHidden,
} from "./StyledComponents";

type InstanceCardAltProps = {
  isLast: boolean;
  instance: InstanceNodeType;
  fromReplicaset: boolean;
  nextNodeIsTierType: boolean;
  nextNodeIsReplicasetType: boolean;
};
export const InstanceCardAlt = memo(
  ({
    isLast,
    instance,
    fromReplicaset,
    nextNodeIsReplicasetType,
    nextNodeIsTierType,
  }: InstanceCardAltProps) => {
    const { translation } = useTranslation();
    const instanceTranslations = translation.pages.instances.list.instanceCard;
    return (
      <InstanceBackground
        className={"item"}
        $variant={"white"}
        $withBottomRadius={isLast}
        $withBottomPadding={isLast}
      >
        <InstanceBackgroundInner
          $variant={fromReplicaset ? "gray" : "white"}
          $withBottomRadius={
            isLast || nextNodeIsReplicasetType || nextNodeIsTierType
          }
          $withBottomPadding={
            isLast || nextNodeIsReplicasetType || nextNodeIsTierType
          }
          $fromReplicaset={fromReplicaset}
        >
          <InstanceItemRoot
            onClick={(event) => {
              event.stopPropagation();
            }}
            $fromReplicaset={fromReplicaset}
          >
            <InstanceCell $fromReplicaset={fromReplicaset}>
              <InstanceNameCell
                $position={"left"}
                $fromReplicaset={fromReplicaset}
              >
                {instance.isLeader ? (
                  <LeaderBlock>{instanceTranslations.leader.label}</LeaderBlock>
                ) : (
                  <FollowerBlock />
                )}
                <ContentFlexCenteredNameCell $fromReplicaset={fromReplicaset}>
                  {!fromReplicaset ? (
                    <CellLabel>{instanceTranslations.name.label}</CellLabel>
                  ) : null}
                  <Ellipsis>
                    <Tooltip title={instance.name}>
                      <>{instance.name}</>
                    </Tooltip>
                  </Ellipsis>
                </ContentFlexCenteredNameCell>
              </InstanceNameCell>
            </InstanceCell>
            <ContentFlexCenteredCell>
              <CellLabel>{instanceTranslations.failureDomain.label}</CellLabel>
              <DomainValue>
                <FailureDomainLabel failureDomain={instance.failureDomain} />
              </DomainValue>
            </ContentFlexCenteredCell>
            <ContentFlexCell></ContentFlexCell>
            <ContentFlexCell></ContentFlexCell>
            <ContentFlexCenteredCell>
              <CellLabel>{instanceTranslations.currentState.label}</CellLabel>
              <Box>
                <NetworkState state={instance.currentState} />
              </Box>
            </ContentFlexCenteredCell>
            <ContentFlexCell></ContentFlexCell>
            <ContentFlexCell>
              <AddressBlock
                addresses={[
                  {
                    title: instanceTranslations.binaryAddress.label,
                    value: instance.binaryAddress,
                  },
                  {
                    title: instanceTranslations.httpAddress.label,
                    value: instance.httpAddress ?? "",
                  },
                  {
                    title: instanceTranslations.pgAddress.label,
                    value: instance.pgAddress,
                  },
                ]}
              />
            </ContentFlexCell>
            <VersionBlock
              label={instanceTranslations.version.label}
              version={instance.version}
              noData={
                <InfoNoData text={translation.components.infoNoData.label} />
              }
            />
            <Cell> </Cell>
          </InstanceItemRoot>
        </InstanceBackgroundInner>
      </InstanceBackground>
    );
  }
);

function VersionBlock(props: {
  label: string;
  version: string;
  noData?: ReactNode;
}) {
  return (
    <ContentFlexCenteredCell>
      <CellLabel>{props.label}</CellLabel>
      <Box>
        {props.version ? (
          <ValueHidden>
            <HiddenWrapper>{props.version}</HiddenWrapper>
          </ValueHidden>
        ) : (
          props.noData
        )}
      </Box>
    </ContentFlexCenteredCell>
  );
}
