import { memo, ReactNode } from "react";
import { Box, Tooltip } from "@mui/material";

import { InstanceNodeType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import {
  Cell,
  CellCenter,
  CellLabel,
  ContentFlexCell,
  ContentFlexCenteredCell,
  Ellipsis,
  LinkCellValue,
  VotersStatusBlock,
} from "../../common";
import { useOpenFullInstanceCard } from "../../FullInstanceCard";

import { FailureDomainLabel } from "./FailureDomainLabel/FailureDomainLabel";
import { AddressBlock } from "./AddressBlock/AddressBlock";
import {
  AddressCell,
  ContentFlexCenteredNameCell,
  DomainValue,
  InstanceBackground,
  InstanceBackgroundInner,
  InstanceCell,
  InstanceItemRoot,
  InstanceNameCell,
  InstanceTypeBlock,
  LeaderBlock,
  RaftLeaderBlock,
  StyledLeaderIcon,
  ValueHidden,
  VersionRoot,
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
    const { openFullInstanceCard } = useOpenFullInstanceCard();

    const instanceNameClickHandler = () => {
      openFullInstanceCard(instance.uuid);
    };

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
                <InstanceTypeBlock>
                  {instance.isRaftLeader ? (
                    <RaftLeaderBlock>
                      <StyledLeaderIcon />
                      <span>raft leader</span>
                    </RaftLeaderBlock>
                  ) : null}
                  {instance.isLeader ? (
                    <LeaderBlock>
                      {instanceTranslations.leader.label}
                    </LeaderBlock>
                  ) : null}
                </InstanceTypeBlock>
                <ContentFlexCenteredNameCell
                  $fromReplicaset={fromReplicaset}
                  onClick={instanceNameClickHandler}
                >
                  {!fromReplicaset ? (
                    <CellLabel>{instanceTranslations.name.label}</CellLabel>
                  ) : null}
                  <Ellipsis>
                    <Tooltip title={instance.name}>
                      <LinkCellValue className={"instance-name"}>
                        {instance.name}
                      </LinkCellValue>
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
            <AddressCell>
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
            </AddressCell>
            <CellCenter>
              {instance.isVoter ? <VotersStatusBlock /> : null}
            </CellCenter>
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
    <VersionRoot>
      <Box></Box>
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
    </VersionRoot>
  );
}
