import React, { FC, ReactNode } from "react";
import { SxProps } from "@mui/material";

import { InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import { NetworkState } from "shared/components/NetworkState/NetworkState";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import { FailureDomainLabel } from "./FailureDomainLabel/FailureDomainLabel";
import { AddressBlock } from "./AddressBlock/AddressBlock";
import {
  CardWrapper,
  Content,
  CurrentStateColumn,
  CurrentStateValue,
  DomainValue,
  FollowerBlock,
  HiddenFailureDomainColumn,
  HiddenNameColumn,
  JoinedColumn,
  Label,
  LeaderBlock,
  StartValue,
  ValueHidden,
  VersionColumn,
} from "./StyledComponents";

interface InstanceCardProps {
  instance: InstanceType;
  theme?: "primary" | "secondary";
  cardWrapperSx?: SxProps;
}

export const InstanceCard: FC<InstanceCardProps> = React.memo(
  ({ instance, theme = "primary", cardWrapperSx = {} }) => {
    const { translation } = useTranslation();
    const instanceTranslations = translation.pages.instances.list.instanceCard;

    return (
      <>
        <CardWrapper
          $theme={theme}
          onClick={(event) => {
            event.stopPropagation();
          }}
          sx={cardWrapperSx}
        >
          {instance.isLeader ? (
            <LeaderBlock>{instanceTranslations.leader.label}</LeaderBlock>
          ) : (
            <FollowerBlock />
          )}
          <Content>
            <HiddenNameColumn>
              {theme === "primary" && (
                <Label $alignLeft={true}>
                  {instanceTranslations.name.label}
                </Label>
              )}
              <StartValue>
                <HiddenWrapper>{instance.name}</HiddenWrapper>
              </StartValue>
            </HiddenNameColumn>
            <HiddenFailureDomainColumn>
              <Label>{instanceTranslations.failureDomain.label}</Label>
              <DomainValue>
                <FailureDomainLabel failureDomain={instance.failureDomain} />
              </DomainValue>
            </HiddenFailureDomainColumn>
            <JoinedColumn>
              <CurrentStateColumn>
                <Label>{instanceTranslations.currentState.label}</Label>
                <CurrentStateValue>
                  <NetworkState state={instance.currentState} />
                </CurrentStateValue>
              </CurrentStateColumn>
            </JoinedColumn>
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
            <VersionBlock
              label={instanceTranslations.version.label}
              version={instance.version}
              noData={
                <InfoNoData text={translation.components.infoNoData.label} />
              }
            />
          </Content>
        </CardWrapper>
        {/* <InstanceModal
          key={`${instance.name}_modal`}
          instance={instance}
          isOpen={isOpenModal}
          onClose={onCloseHandler}
        /> */}
      </>
    );
  }
);

function VersionBlock(props: {
  label: string;
  version: string;
  noData?: ReactNode;
}) {
  return (
    <VersionColumn>
      <Label>{props.label}</Label>
      {props.version ? (
        <ValueHidden>
          <HiddenWrapper>{props.version}</HiddenWrapper>
        </ValueHidden>
      ) : (
        props.noData
      )}
    </VersionColumn>
  );
}
