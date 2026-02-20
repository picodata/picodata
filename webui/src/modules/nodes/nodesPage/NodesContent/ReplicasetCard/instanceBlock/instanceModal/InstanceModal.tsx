import { FC, useMemo } from "react";
import { createPortal } from "react-dom";

import { CloseIcon } from "shared/icons/CloseIcon";
import { LeaderBigIcon } from "shared/icons/LeaderBigIcon";
import { InstanceType } from "shared/entity/instance";

import {
  Body,
  BoxInfo,
  BoxInfoRaw,
  BoxInfoWrapper,
  CloseElement,
  starIconStyle,
  TabGroup,
  TitleText,
  TitleTextInline,
  TitleWrapper,
  Wrapper,
} from "./StyledComponents";

export interface InstanceModalProps {
  isOpen: boolean;
  onClose: () => void;
  instance: InstanceType;
}

export const InstanceModal: FC<InstanceModalProps> = ({
  isOpen,
  onClose,
  instance,
}) => {
  const boxInfoEl = useMemo(() => {
    const keys = Object.keys(instance);
    return (
      <BoxInfoWrapper>
        {keys.map((key, index) => {
          if (typeof instance[key as keyof InstanceType] === "string") {
            return (
              <BoxInfoRaw key={index} $isGrayRow={index % 2 !== 0}>
                <span>
                  <TitleText>{key}</TitleText>
                </span>
                <p>{instance[key as keyof InstanceType]?.toString()}</p>
              </BoxInfoRaw>
            );
          }
        })}
      </BoxInfoWrapper>
    );
  }, [instance]);

  if (!isOpen) {
    return null;
  }

  return createPortal(
    <Wrapper
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
      }}
    >
      <Body>
        <TitleWrapper>
          <TitleTextInline>
            {instance.isLeader && <LeaderBigIcon style={starIconStyle} />}
            {instance.name}
          </TitleTextInline>
          <CloseElement
            onClick={(event) => {
              event.stopPropagation();
              onClose();
            }}
          >
            <CloseIcon />
          </CloseElement>
        </TitleWrapper>
        <TabGroup>
          <span>General</span>
        </TabGroup>
        <BoxInfo>{boxInfoEl}</BoxInfo>
      </Body>
    </Wrapper>,
    document.body
  );
};
