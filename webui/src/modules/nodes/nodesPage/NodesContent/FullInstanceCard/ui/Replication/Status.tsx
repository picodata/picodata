import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { DownstreamStatus, UpstreamStatus } from "shared/entity/instance";
export const StatusCircle = styled(Box)<{
  $status: DownstreamStatus | UpstreamStatus;
}>(({ theme, $status }) => {
  let color: string = theme.palette.success.main;
  if ($status !== "follow") {
    color = theme.palette.error.main;
  }
  return {
    width: 10,
    height: 10,
    borderRadius: "50%",
    background: "red",
    backgroundColor: color,
  };
});
const Root = styled(Box)<{ $direction?: "column" | "row" }>(
  ({ $direction = "column" }) => ({
    display: "flex",
    gap: 6,
    flexDirection: $direction,
    alignItems: "center",
    justifyContent: "center",
  })
);

type StreamStatusProps = {
  status: DownstreamStatus | UpstreamStatus;
  direction?: "column" | "row";
};
export const StreamStatus = ({
  status,
  direction = "column",
}: StreamStatusProps) => {
  return (
    <Root $direction={direction}>
      <StatusCircle $status={status} />
      <Box fontSize={12} color={grey[700]}>
        {status}
      </Box>
    </Root>
  );
};

const InactiveStatusCircle = styled(Box)({
  width: 10,
  height: 10,
  border: `1px solid ${grey[500]}`,
  borderRadius: "50%",
});

const InactiveStatusText = styled(Box)({
  color: grey[600],
  fontSize: 12,
  textAlign: "center",
  whiteSpace: "nowrap",
});

export const InactiveStreamStatus = ({ label }: { label: string }) => {
  return (
    <Root>
      <InactiveStatusCircle />
      <InactiveStatusText>{label}</InactiveStatusText>
    </Root>
  );
};
