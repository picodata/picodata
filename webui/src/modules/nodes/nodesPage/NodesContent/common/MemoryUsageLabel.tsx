import { Box, styled } from "@mui/material";

import { InfoTooltip } from "shared/ui/InfoTooltip/InfoTooltip";
import { useTranslation } from "shared/intl";

import { CellLabel } from "./ItemRoot";

const TooltipContent = styled(Box)({
  display: "flex",
  flexDirection: "column",
  gap: 6,
  maxWidth: 260,
});

const TooltipTitle = styled(Box)({
  fontWeight: 600,
});

type MemoryUsageLabelProps = {
  scope: "replicaset" | "tier";
};

export const MemoryUsageLabel = ({ scope }: MemoryUsageLabelProps) => {
  const { translation } = useTranslation();
  const memoryTranslations = translation.pages.instances.list.common.memory;
  const description =
    scope === "replicaset"
      ? memoryTranslations.infoDescriptionReplicaset
      : memoryTranslations.infoDescriptionTier;

  return (
    <CellLabel>
      {memoryTranslations.label}
      <InfoTooltip
        title={
          <TooltipContent>
            <TooltipTitle>{memoryTranslations.infoTitle}</TooltipTitle>
            <Box>{description}</Box>
            <Box>{memoryTranslations.infoFreedMemoryNote}</Box>
            <Box>{memoryTranslations.infoRestartNote}</Box>
          </TooltipContent>
        }
      />
    </CellLabel>
  );
};
