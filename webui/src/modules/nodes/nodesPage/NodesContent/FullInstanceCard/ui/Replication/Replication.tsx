import { Autocomplete, Box, styled, TextField } from "@mui/material";
import { useState } from "react";
import { grey } from "@mui/material/colors";

import { InstanceReplication, InstanceType } from "shared/entity/instance";
import { HiddenBox } from "shared/ui/HiddenBox";
import { useTranslation } from "shared/intl";

import { useReplicationInstances } from "../../lib";

import { StatusCircle } from "./Status";
import { Schema } from "./Schema";
import { Label, Value } from "./Common";
import { CommonStream } from "./CommonStream";
import { VClock } from "./VClock";

const Stream = styled(HiddenBox)({
  width: "100%",
  padding: 10,
  display: "flex",
  flexDirection: "column",
});

const OptionStatuses = styled(Box)({
  display: "flex",
  gap: 10,
});
const OptionRoot = styled(Box)({
  display: "flex",
  justifyContent: "space-between",
  gap: 10,
  width: "100%",
});
const OptionStatusText = styled(Box)({
  color: grey[700],
  fontWeight: 100,
  fontSize: 12,
  border: `1px solid ${grey[500]}`,
  padding: "0 10px",
  borderRadius: 4,
  display: "flex",
  alignItems: "center",
  gap: 6,
});
const AutoCompleteOption = ({
  downstream,
  upstream,
  id,
}: InstanceReplication) => {
  return (
    <OptionRoot>
      <Box>{id}</Box>
      <OptionStatuses>
        {downstream ? (
          <OptionStatusText>
            <Box>downstream</Box>
            <StatusCircle $status={downstream.status} />
          </OptionStatusText>
        ) : null}
        {upstream ? (
          <OptionStatusText>
            upstream
            <StatusCircle $status={upstream.status} />
          </OptionStatusText>
        ) : null}
      </OptionStatuses>
    </OptionRoot>
  );
};

const Root = styled(Box)({
  height: "100%",
  padding: "16px 30px",
  display: "grid",
  gridTemplateRows: "min-content 1fr 1fr",
  gridTemplateColumns: "1fr 1fr",
  gridTemplateAreas: `
    "select select"
    "schema  downstream"
    "schema  upstream"
  `,
  gap: 10,
});

const Area = styled(Box)({
  border: `1px solid ${grey[400]}`,
  borderRadius: 10,
});
type ReplicationProps = {
  replications: Record<number, InstanceReplication>;
  currentInstanceId: string;
  instances: InstanceType[];
};

export const Replication = ({
  instances,
  replications,
  currentInstanceId,
}: ReplicationProps) => {
  const { translation } = useTranslation();
  const replicationContentTranslation =
    translation.pages.instances.list.fullInstanceCard.replicationContent;

  const [currentInstance, otherInstances] = useReplicationInstances(
    replications,
    currentInstanceId
  );

  const [selectedInstance, setSelectedInstance] = useState<InstanceReplication>(
    otherInstances[0]
  );

  return (
    <Root>
      <Box gridArea={"select"}>
        <Autocomplete
          options={otherInstances}
          value={selectedInstance}
          renderOption={(props, option) => (
            <Box component="li" {...props}>
              <AutoCompleteOption {...option} />
            </Box>
          )}
          renderValue={({ id }) => <Box paddingLeft={1}>{id}</Box>}
          renderInput={(params) => (
            <TextField
              variant={"outlined"}
              {...params}
              label={replicationContentTranslation.remoteInstance}
            />
          )}
          onChange={(_, value) => {
            setSelectedInstance(value);
          }}
          disableClearable={true}
        />
      </Box>
      <Box gridArea={"schema"}>
        <Schema
          instances={instances}
          localInstance={currentInstance}
          remoteInstance={selectedInstance}
        />
      </Box>
      {selectedInstance?.downstream ? (
        <Area gridArea={"downstream"}>
          <Stream>
            <CommonStream stream={selectedInstance.downstream} type={"down"} />
            {selectedInstance.downstream.vclock ? (
              <Box>
                <Label>Vclock</Label>
                <VClock vclock={selectedInstance.downstream.vclock} />
              </Box>
            ) : null}
          </Stream>
        </Area>
      ) : null}
      {selectedInstance?.upstream ? (
        <Area gridArea={"upstream"}>
          <Stream>
            <CommonStream stream={selectedInstance.upstream} type={"up"} />
            <Box>
              <Label>peer</Label>
              <Value>{selectedInstance.upstream.peer}</Value>
            </Box>
          </Stream>
        </Area>
      ) : null}
    </Root>
  );
};
