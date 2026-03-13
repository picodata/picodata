import { Box, Tooltip } from "@mui/material";
import { memo } from "react";

import { ChevronDown } from "shared/icons/ChevronDown";
import { TierNodeType } from "shared/entity/tier";
import { SwitchInfo } from "shared/ui/SwitchInfo/SwitchInfo";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";

import { CapacityProgress } from "../../ClusterInfo/CapacityProgress/CapacityProgress";
import { CellLabel, CellValue, ContentFlexCell } from "../common";

import {
  chevronIconIsOpenStyle,
  chevronIconStyle,
  ServicesList,
  TierBackground,
  TierCapacityProgressCell,
  TierContentFlexCenteredCell,
  TierItemRoot,
} from "./StyledComponents";

type TierCardAltProps = {
  isLast: boolean;
  isFirst: boolean;
  theme?: "primary" | "secondary";
  tier: TierNodeType;
  onClick: (id: string) => void;
};
export const TierCardAlt = memo(
  ({ isLast, isFirst, tier, theme, onClick }: TierCardAltProps) => {
    const { translation } = useTranslation();
    const tierTranslations = translation.pages.instances.list.tierCard;

    const tierClickHandler = () => {
      onClick(tier.syntheticId);
    };
    return (
      <TierBackground
        className={"item"}
        $withPaddingBottom={isLast}
        $withPaddingTop={!isFirst}
        $withBottomRadius={isLast}
        $variant={"white"}
      >
        <TierItemRoot onClick={tierClickHandler} $withBorderRadius={!tier.open}>
          <Box></Box>
          <ContentFlexCell>
            <CellLabel>{tierTranslations.name.label}</CellLabel>
            <Tooltip title={tier.name}>
              <CellValue>{tier.name}</CellValue>
            </Tooltip>
          </ContentFlexCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.services.label}</CellLabel>
            <ServicesList>
              {tier.services.length ? (
                tier.services.map((service) => (
                  <Tooltip key={service} title={service}>
                    <CellValue>{service}</CellValue>
                  </Tooltip>
                ))
              ) : (
                <InfoNoData text={translation.components.infoNoData.label} />
              )}
            </ServicesList>
          </TierContentFlexCenteredCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.replicasets.label}</CellLabel>
            <CellValue>{tier.replicasetCount}</CellValue>
          </TierContentFlexCenteredCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.instances.label}</CellLabel>
            <CellValue>{tier.instanceCount}</CellValue>
          </TierContentFlexCenteredCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.rf.label}</CellLabel>
            <CellValue>{tier.rf}</CellValue>
          </TierContentFlexCenteredCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.bucket_count.label}</CellLabel>
            <CellValue>{tier.bucketCount}</CellValue>
          </TierContentFlexCenteredCell>

          <TierContentFlexCenteredCell>
            <CellLabel>{tierTranslations.canVote.label}</CellLabel>
            <SwitchInfo checked={tier.can_vote} />
          </TierContentFlexCenteredCell>

          <TierCapacityProgressCell>
            <CapacityProgress
              percent={tier.capacityUsage}
              currentValue={tier.memory?.used ?? 0}
              limit={tier.memory?.usable ?? 0}
              size="small"
              theme={theme === "secondary" ? "primary" : "secondary"}
              progressLineWidth="100%"
            />
          </TierCapacityProgressCell>
          <TierContentFlexCenteredCell>
            <ChevronDown
              style={tier.open ? chevronIconIsOpenStyle : chevronIconStyle}
            />
          </TierContentFlexCenteredCell>
          <Box></Box>
        </TierItemRoot>
      </TierBackground>
    );
  }
);
