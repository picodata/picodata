import React, { FC, useState } from "react";

import { ChevronDown } from "shared/icons/ChevronDown";
import { TierType } from "shared/entity/tier";
import { SwitchInfo } from "shared/ui/SwitchInfo/SwitchInfo";
import { Collapse } from "shared/ui/Collapse/Collapse";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

import { ReplicasetCard } from "../ReplicasetCard/ReplicasetCard";
import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";

import {
  BucketCountColumn,
  CanVoterColumn,
  CapacityColumn,
  CardWrapper,
  ChevronColumn,
  chevronIconIsOpenStyle,
  chevronIconStyle,
  Content,
  HiddenInfoValue,
  InfoValue,
  InstancesColumn,
  Label,
  NameColumn,
  ReplicasetsColumn,
  ReplicasetsWrapper,
  RfColumn,
  ServicesColumn,
  ServicesValue,
} from "./StyledComponents";

export interface TierCardProps {
  theme?: "primary" | "secondary";
  tier: TierType;
}

export const TierCard: FC<TierCardProps> = React.memo(({ tier, theme }) => {
  const [isOpen, setIsOpen] = useState<boolean>(false);

  const { translation } = useTranslation();
  const tierTranslations = translation.pages.instances.list.tierCard;

  const onClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
    event.stopPropagation();
    setIsOpen(!isOpen);
  };

  return (
    <CardWrapper onClick={onClick}>
      <Content>
        <NameColumn>
          <Label>{tierTranslations.name.label}</Label>
          <HiddenInfoValue>
            <HiddenWrapper>{tier.name}</HiddenWrapper>
          </HiddenInfoValue>
        </NameColumn>
        <ServicesColumn>
          <Label>{tierTranslations.services.label}</Label>
          <ServicesValue>
            {tier.services.length ? (
              <HiddenWrapper>
                {tier.services.map((service, i) =>
                  i == 0 ? (
                    <>{service}</>
                  ) : (
                    <>
                      <br />
                      {service}
                    </>
                  )
                )}
              </HiddenWrapper>
            ) : (
              <InfoNoData text={translation.components.infoNoData.label} />
            )}
          </ServicesValue>
        </ServicesColumn>
        <ReplicasetsColumn>
          <Label>{tierTranslations.replicasets.label}</Label>
          <InfoValue>{tier.replicasetCount}</InfoValue>
        </ReplicasetsColumn>
        <InstancesColumn>
          <Label>{tierTranslations.instances.label}</Label>
          <InfoValue>{tier.instanceCount}</InfoValue>
        </InstancesColumn>
        <RfColumn>
          <Label>{tierTranslations.rf.label}</Label>
          <InfoValue>{tier.rf}</InfoValue>
        </RfColumn>
        <BucketCountColumn>
          <Label>{tierTranslations.bucket_count.label}</Label>
          <InfoValue>{tier.bucketCount}</InfoValue>
        </BucketCountColumn>
        <CanVoterColumn>
          <Label>{tierTranslations.canVote.label}</Label>
          <InfoValue>
            <SwitchInfo checked={tier.can_vote} />
          </InfoValue>
        </CanVoterColumn>
        {tier.memory && (
          <CapacityColumn>
            <CapacityProgress
              percent={tier.capacityUsage}
              currentValue={tier.memory?.used ?? 0}
              limit={tier.memory?.usable ?? 0}
              size="small"
              theme={theme === "secondary" ? "primary" : "secondary"}
              progressLineWidth="100%"
            />
          </CapacityColumn>
        )}
        <ChevronColumn>
          <ChevronDown
            style={isOpen ? chevronIconIsOpenStyle : chevronIconStyle}
          />
        </ChevronColumn>
      </Content>
      <Collapse isOpen={isOpen}>
        <ReplicasetsWrapper>
          {tier.replicasets.map((replicaset) => (
            <ReplicasetCard
              key={replicaset.name}
              replicaset={replicaset}
              theme="secondary"
            />
          ))}
        </ReplicasetsWrapper>
      </Collapse>
    </CardWrapper>
  );
});
