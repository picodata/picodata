import React, { FC, useState } from "react";

import { ChevronDown } from "shared/icons/ChevronDown";
import { InstanceType } from "shared/entity/instance";
import { Collapse } from "shared/ui/Collapse/Collapse";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";

import { InstanceCard } from "./instanceBlock/InstanceCard";
import {
  CapacityColumn,
  CardWrapperTheme,
  ChevronColumn,
  chevronIconIsOpenStyle,
  chevronIconStyle,
  Content,
  HiddenInfoValue,
  InfoStateColumn,
  InfoValue,
  instancesCardWrapperSx,
  InstancesColumn,
  InstancesWrapper,
  Label,
  NameColumn,
  StateColumn,
  StateInfoValue,
} from "./StyledComponents";

export type TReplicaset = {
  name: string;
  instanceCount: number;
  instances: InstanceType[];
  version: string;
  state: "Online" | "Offline" | "Expelled";
  capacityUsage: number;
  memory: {
    usable: number;
    used: number;
  };
};

export interface ReplicasetCardProps {
  theme?: "primary" | "secondary";
  replicaset: TReplicaset;
}

export const ReplicasetCard: FC<ReplicasetCardProps> = React.memo(
  ({ replicaset, theme = "primary" }) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);

    const { translation } = useTranslation();
    const replicasetTranslations =
      translation.pages.instances.list.replicasetCard;

    const onClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
      event.stopPropagation();
      setIsOpen(!isOpen);
    };

    return (
      <CardWrapperTheme
        // className={cn(styles.cardWrapper, styles[theme])}
        $theme={theme}
        onClick={onClick}
      >
        <Content $theme={theme}>
          <NameColumn>
            <Label>{replicasetTranslations.name.label}</Label>
            <HiddenInfoValue>
              <HiddenWrapper>{replicaset.name}</HiddenWrapper>
            </HiddenInfoValue>
          </NameColumn>
          <InstancesColumn>
            <Label>{replicasetTranslations.instances.label}</Label>
            <InfoValue>{replicaset.instanceCount}</InfoValue>
          </InstancesColumn>
          <InfoStateColumn>
            <Label>{replicasetTranslations.state.label}</Label>
            <StateInfoValue>
              <NetworkState state={replicaset.state} />
            </StateInfoValue>
          </InfoStateColumn>
          <StateColumn />
          <CapacityColumn>
            <CapacityProgress
              percent={replicaset.capacityUsage}
              currentValue={replicaset.memory.used}
              limit={replicaset.memory.usable}
              size="small"
              theme={theme === "secondary" ? "primary" : "secondary"}
              progressLineWidth="100%"
            />
          </CapacityColumn>
          <ChevronColumn>
            <ChevronDown
              style={isOpen ? chevronIconIsOpenStyle : chevronIconStyle}
            />
          </ChevronColumn>
        </Content>
        <Collapse isOpen={isOpen}>
          <InstancesWrapper $theme={theme}>
            {replicaset.instances.map((instance) => (
              <InstanceCard
                cardWrapperSx={
                  theme === "secondary" ? instancesCardWrapperSx : undefined
                }
                key={instance.name}
                instance={instance}
                theme="secondary"
              />
            ))}
          </InstancesWrapper>
        </Collapse>
      </CardWrapperTheme>
    );
  }
);
