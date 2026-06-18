import { Box, Tooltip } from "@mui/material";

import { DownStream, UpStream } from "shared/entity/instance";
import { useTranslation } from "shared/intl";

import { Label, Value } from "./Common";
import { NoDataStream } from "./NoDataStream";
import { StreamStatus } from "./Status";

type CommonStreamProps = {
  stream: UpStream | DownStream;
  type: "up" | "down";
};
export const CommonStream = ({ stream, type }: CommonStreamProps) => {
  const { translation } = useTranslation();
  const replicationContentTranslation =
    translation.pages.instances.list.fullInstanceCard.replicationContent;

  return stream ? (
    <Box>
      <Box display={"flex"} gap={"10px"}>
        <Tooltip
          title={
            type === "up"
              ? replicationContentTranslation.upStreamDescription
              : replicationContentTranslation.downStreamDescription
          }
        >
          <Label>{type === "up" ? "upstream" : "downstream"}</Label>
        </Tooltip>
        <StreamStatus status={stream.status} direction={"row"} />
      </Box>

      {typeof stream?.idle === "number" ? (
        <Box>
          <Label>idle</Label>
          <Value>{stream?.idle}</Value>
        </Box>
      ) : null}

      {typeof stream?.lag === "number" ? (
        <Box>
          <Label>lag</Label>
          <Value>{stream?.lag}</Value>
        </Box>
      ) : null}
    </Box>
  ) : (
    <NoDataStream />
  );
};
