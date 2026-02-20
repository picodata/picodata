import { Tooltip } from "@mui/material";
import { memo } from "react";

import { ChevronDown } from "shared/icons/ChevronDown";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";

import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";
import {
  CapacityProgressCell,
  CellLabel,
  CellValue,
  ContentFlexCell,
  ContentFlexCenteredCell,
} from "../common";
import { ReplicasetNodeType } from "../../../../../shared/entity/replicaset";

import {
  chevronIconIsOpenStyle,
  chevronIconStyle,
  ReplicasetBackground,
  ReplicasetIconCell,
  ReplicasetInnerBackground,
  ReplicasetItemRoot,
} from "./StyledComponents";

type ReplicasetCardAltProps = {
  isLast: boolean;
  replicaset: ReplicasetNodeType;
  nextNodeIsTierType: boolean;
  onClick: (id: string) => void;
};
export const ReplicasetCardAlt = memo(
  ({
    isLast,
    replicaset,
    onClick,
    nextNodeIsTierType,
  }: ReplicasetCardAltProps) => {
    const { translation } = useTranslation();
    const replicasetTranslations =
      translation.pages.instances.list.replicasetCard;

    const clickHandler = (
      event: React.MouseEvent<HTMLDivElement, MouseEvent>
    ) => {
      event.stopPropagation();
      onClick(replicaset.syntheticId);
    };

    return (
      <ReplicasetBackground
        className={"item"}
        $variant={"white"}
        $withBottomPadding={isLast}
        $withBottomRadius={isLast}
      >
        <ReplicasetInnerBackground
          $variant={"gray"}
          $withBottomPadding={nextNodeIsTierType || isLast}
          $withBottomRadius={nextNodeIsTierType || isLast}
        >
          <ReplicasetItemRoot
            onClick={clickHandler}
            $withBotomRadius={!replicaset.open}
          >
            <ContentFlexCell>
              <CellLabel>{replicasetTranslations.name.label}</CellLabel>
              <Tooltip title={replicaset.name}>
                <CellValue>{replicaset.name}</CellValue>
              </Tooltip>
            </ContentFlexCell>

            <ContentFlexCenteredCell></ContentFlexCenteredCell>

            <ContentFlexCenteredCell></ContentFlexCenteredCell>

            <ContentFlexCenteredCell>
              <CellLabel>{replicasetTranslations.instances.label}</CellLabel>
              <CellValue>{replicaset.instanceCount}</CellValue>
            </ContentFlexCenteredCell>

            <ContentFlexCenteredCell>
              <CellLabel>{replicasetTranslations.state.label}</CellLabel>
              <NetworkState state={replicaset.state} />
            </ContentFlexCenteredCell>

            <ContentFlexCenteredCell></ContentFlexCenteredCell>

            <ContentFlexCenteredCell></ContentFlexCenteredCell>

            <CapacityProgressCell>
              <CapacityProgress
                percent={replicaset.capacityUsage}
                currentValue={replicaset.memory.used}
                limit={replicaset.memory.usable}
                size="small"
                theme={"primary"}
                progressLineWidth="100%"
              />
            </CapacityProgressCell>
            <ReplicasetIconCell>
              <ContentFlexCenteredCell>
                <ChevronDown
                  style={
                    replicaset.open ? chevronIconIsOpenStyle : chevronIconStyle
                  }
                />
              </ContentFlexCenteredCell>
            </ReplicasetIconCell>
          </ReplicasetItemRoot>
        </ReplicasetInnerBackground>
      </ReplicasetBackground>
    );
  }
);
