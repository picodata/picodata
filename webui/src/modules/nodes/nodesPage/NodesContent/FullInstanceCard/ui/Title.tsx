import { Box, styled } from "@mui/material";
import { green, grey } from "@mui/material/colors";
import TourIcon from "@mui/icons-material/Tour";

import { InstanceType } from "shared/entity/instance";
import { Leader } from "shared/icons";
import { useTranslation } from "shared/intl";

const RaftLeaderIcon = styled(Leader)({
  width: 16,
  height: 16,
});
export const LeaderIcon = styled(TourIcon)({
  fill: green[600],
  width: 18,
  height: 18,
  transform: "translateY(1px)",
});

const Root = styled(Box)({
  fontSize: 18,
  display: "flex",
  gap: 10,
  alignItems: "center",
  paddingLeft: 24,
  paddingBottom: 10,
});
const StatusIcons = styled(Box)({
  display: "flex",
  gap: 4,
  alignItems: "center",
});
const PrevText = styled(Box)({
  color: grey[600],
});

type TitleProps = {
  instance: InstanceType;
};
export const Title = ({ instance }: TitleProps) => {
  const { translation } = useTranslation();

  return (
    <Root>
      <PrevText>
        {translation.pages.instances.list.fullInstanceCard.instance.label}
      </PrevText>
      <Box>{instance.name}</Box>
      <StatusIcons>
        {instance.isLeader && (
          <Box>
            <LeaderIcon />
          </Box>
        )}
        {instance.isRaftLeader && (
          <Box>
            <RaftLeaderIcon />
          </Box>
        )}
      </StatusIcons>
    </Root>
  );
};
