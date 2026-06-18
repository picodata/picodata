import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { InstanceReplication, InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";

import { Label } from "./Common";
import { StreamArrow } from "./StreamArrow";
import { StreamStatus } from "./Status";

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
        <Box fontWeight={"bold"}>
          {instance?.name || commonTranslation.noData}
        </Box>
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
        ) : null}
        {remoteInstance.upstream ? (
          <>
            <StreamArrow direction={"up"} />
            <StreamStatus status={remoteInstance.upstream.status} />
          </>
        ) : null}
      </ArrowContainer>
      <SchemaInstanceContainer alignItems={"flex-start"}>
        <SchemaInstance replication={remoteInstance} instances={instances} />
      </SchemaInstanceContainer>
    </Root>
  );
};
