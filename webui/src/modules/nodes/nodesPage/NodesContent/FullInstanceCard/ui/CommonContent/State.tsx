import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { InstanceState } from "shared/entity/instance";

const Circle = styled(Box)<{ $state: InstanceState }>(({ theme, $state }) => {
  let color: string;
  switch ($state) {
    case "Online": {
      color = theme.palette.success.main;
      break;
    }
    case "Offline": {
      color = theme.palette.error.main;
      break;
    }
    default:
      color = grey[400];
  }
  return {
    width: 10,
    height: 10,
    borderRadius: "50%",
    background: color,
  };
});

const Root = styled(Box)({
  display: "flex",
  alignItems: "center",
  gap: 6,
  color: grey[600],
});
type StateProps = {
  value: InstanceState;
};
export const State = ({ value }: StateProps) => {
  return (
    <Root>
      <Circle $state={value} />
      <Box>{value}</Box>
    </Root>
  );
};
