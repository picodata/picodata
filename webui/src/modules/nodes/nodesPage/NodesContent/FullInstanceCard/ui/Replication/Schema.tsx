import { Box, styled, Tooltip } from "@mui/material";
import { grey } from "@mui/material/colors";

import { InstanceReplication, InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";

import { Leader } from "../../../../../../../shared/icons";

import { Label } from "./Common";
import { StreamArrow } from "./StreamArrow";
import { InactiveStreamStatus, StreamStatus } from "./Status";

const ArrowContainer = styled(Box)({
  display: "flex",
  height: "100%",
  justifyContent: "center",
  alignItems: "center",
  gap: 4,
});

const SchemaInstanceRoot = styled(Box)({
  width: 180,
  gap: 6,
  display: "flex",
  flexDirection: "column",
});
const SchemaInstanceBody = styled(Box)<{ $isLocal?: boolean }>(
  ({ theme, $isLocal }) => ({
    height: 60,
    border: `1px solid ${$isLocal ? theme.palette.primary.main : grey[400]}`,
    borderRadius: 10,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    gap: 2,
    fontSize: 12,
  })
);
const SchemaInstanceHeader = styled(Box)<{ $isLocal?: boolean }>(
  ({ theme, $isLocal }) => ({
    display: "flex",
    justifyContent: "center",
    color: $isLocal ? theme.palette.primary.dark : grey[800],
    fontSize: 14,
    fontWeight: 100,
  })
);

export const StyledLeaderIcon = styled(Leader)({
  width: 14,
  height: 14,
  "& > rect": {
    fill: "green",
  },
});
export const IconContainer = styled(Box)({
  display: "flex",
  alignItems: "center",
});
export const Name = styled(Box)({
  fontWeight: "bold",
  display: "flex",
  alignItems: "center",
  gap: "6px",
});

type SchemaInstanceProps = {
  replication: InstanceReplication | null;
  instances: InstanceType[];
  isLocal?: boolean;
};
export const SchemaInstance = ({
  replication,
  instances,
  isLocal,
}: SchemaInstanceProps) => {
  const { translation } = useTranslation();
  const replicationContentTranslation =
    translation.pages.instances.list.fullInstanceCard.replicationContent;
  const instanceTranslations = translation.pages.instances.list.instanceCard;
  const commonTranslation = translation.common;

  const instance = instances.find(({ uuid }) => uuid === replication?.uuid);

  return (
    <SchemaInstanceRoot>
      <SchemaInstanceHeader $isLocal={isLocal}>
        {isLocal
          ? replicationContentTranslation.currentInstance
          : replicationContentTranslation.remoteInstance}{" "}
        {replication ? `(id: ${replication.id})` : null}
      </SchemaInstanceHeader>
      <SchemaInstanceBody $isLocal={isLocal}>
        <Name>
          {instance?.name || commonTranslation.noData}
          {instance?.isLeader ? (
            <Tooltip title={instanceTranslations.leader.label}>
              <IconContainer>
                <StyledLeaderIcon />
              </IconContainer>
            </Tooltip>
          ) : null}
        </Name>
        <Box>
          <Label>LSN:</Label>{" "}
          {replication && typeof replication.lsn === "number"
            ? replication.lsn
            : "-"}
        </Box>
      </SchemaInstanceBody>
    </SchemaInstanceRoot>
  );
};

const SchemaInstanceContainer = styled(Box)({
  display: "flex",
  justifyContent: "center",
});

const Root = styled(Box)({
  height: "100%",
  display: "grid",
  gridTemplateRows: "2fr 1fr 2fr",
  gap: 6,
});
type SchemaProps = {
  localInstance: InstanceReplication | null;
  remoteInstance: InstanceReplication;
  instances: InstanceType[];
};
export const Schema = ({
  localInstance,
  remoteInstance,
  instances,
}: SchemaProps) => {
  const { translation } = useTranslation();
  const commonTranslation = translation.common;

  return (
    <Root>
      <SchemaInstanceContainer alignItems={"flex-end"}>
        <SchemaInstance
          replication={localInstance}
          instances={instances}
          isLocal={true}
        />
      </SchemaInstanceContainer>
      <ArrowContainer>
        {remoteInstance.downstream ? (
          <>
            <StreamStatus status={remoteInstance.downstream.status} />
            <StreamArrow />
          </>
        ) : (
          <>
            <InactiveStreamStatus label={commonTranslation.noData} />
            <StreamArrow inactive />
          </>
        )}
        {remoteInstance.upstream ? (
          <>
            <StreamArrow direction={"up"} />
            <StreamStatus status={remoteInstance.upstream.status} />
          </>
        ) : (
          <>
            <StreamArrow direction={"up"} inactive />
            <InactiveStreamStatus label={commonTranslation.noData} />
          </>
        )}
      </ArrowContainer>
      <SchemaInstanceContainer alignItems={"flex-start"}>
        <SchemaInstance replication={remoteInstance} instances={instances} />
      </SchemaInstanceContainer>
    </Root>
  );
};
