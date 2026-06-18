import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { FullInstance, InstanceType } from "shared/entity/instance";
import { useTranslation } from "shared/intl";
import {
  getConfigurationLink,
  getGlossaryLink,
  getRaftFailoverLink,
} from "shared/config";

import {
  Section,
  SectionField,
  SectionFieldsContent,
  Address,
} from "../common";

import { State } from "./State";
import { FolderName } from "./FolderName";
import { StatusCheck } from "./StatusCheck";

const border = `1px solid ${grey[200]}`;

const Root = styled(Box)({
  display: "grid",
  gridTemplateColumns: "minmax(auto, 360px) auto",
  paddingLeft: 20,
  paddingRight: 20,
});

type CommonContentProps = {
  instance: InstanceType;
  fullInstance: FullInstance;
};
export const CommonContent = ({
  instance,
  fullInstance,
}: CommonContentProps) => {
  const { translation } = useTranslation();
  const commontContentTranslation =
    translation.pages.instances.list.fullInstanceCard.commonContent;
  return (
    <Root>
      <Box padding={"17px"} borderRight={border} borderBottom={border}>
        <Section title={commontContentTranslation.basic}>
          <SectionFieldsContent>
            <SectionField name={commontContentTranslation.name}>
              {instance.name}
            </SectionField>
            <SectionField name={"UUID"}>{instance.uuid}</SectionField>
            <SectionField name={"Raft ID"}>{fullInstance.raftId}</SectionField>
            <SectionField name={commontContentTranslation.tier}>
              {fullInstance.tier}
            </SectionField>
            <SectionField name={commontContentTranslation.replicaset}>
              {fullInstance.replicasetName}
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
      <Box padding={"17px"} borderBottom={border}>
        <Section title={commontContentTranslation.folders}>
          <SectionFieldsContent>
            <SectionField
              title={fullInstance.instanceDir}
              name={<FolderName value={"instance_dir"} />}
              link={getConfigurationLink("#instance_instance_dir")}
            >
              <Address value={fullInstance.instanceDir} />
            </SectionField>
            <SectionField
              title={fullInstance.backupDir}
              name={<FolderName value={"backup_dir"} />}
              link={getConfigurationLink("#instance_backup_dir")}
            >
              <Address value={fullInstance.backupDir} />
            </SectionField>
            <SectionField
              title={fullInstance.adminSocket}
              name={<FolderName value={"admin_socket"} />}
              link={getConfigurationLink("#instance_admin_socket")}
            >
              <Address value={fullInstance.adminSocket} />
            </SectionField>
            <SectionField
              title={fullInstance.shareDir}
              name={<FolderName value={"share_dir"} />}
              link={getConfigurationLink("#instance_share_dir")}
            >
              <Address value={fullInstance.shareDir} />
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
      <Box padding={"17px"} borderRight={border} borderBottom={border}>
        <Section title={commontContentTranslation.addresses}>
          <SectionFieldsContent>
            <SectionField
              title={instance.binaryAddress}
              name={
                translation.pages.instances.list.instanceCard.binaryAddress
                  .label
              }
              link={getConfigurationLink("#instance_iproto_listen")}
            >
              <Address value={instance.binaryAddress} />
            </SectionField>
            <SectionField
              title={instance.httpAddress}
              name={
                translation.pages.instances.list.instanceCard.httpAddress.label
              }
              link={getConfigurationLink("#instance_http_listen")}
            >
              <Address value={instance.httpAddress} />
            </SectionField>
            <SectionField
              title={instance.pgAddress}
              name={
                translation.pages.instances.list.instanceCard.pgAddress.label
              }
              link={getConfigurationLink("#instance_pgproto_listen")}
            >
              <Address value={instance.pgAddress} />
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
      <Box padding={"17px"} borderBottom={border}>
        <Section title={commontContentTranslation.statuses}>
          <SectionFieldsContent>
            <SectionField
              name={commontContentTranslation.raftLeader}
              withTitle={false}
              link={getGlossaryLink("#raft_leader")}
            >
              <StatusCheck checked={instance.isRaftLeader} />
            </SectionField>
            <SectionField
              name={commontContentTranslation.leader}
              withTitle={false}
              link={getGlossaryLink("#leader")}
            >
              <StatusCheck checked={instance.isLeader} />
            </SectionField>
            <SectionField
              name={commontContentTranslation.voter}
              withTitle={false}
              link={getRaftFailoverLink("#raft_voter_failover")}
            >
              <StatusCheck checked={instance.isVoter} />
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
      <Box padding={"17px"} borderRight={border} borderBottom={border}>
        <Section title={commontContentTranslation.log}>
          <SectionFieldsContent>
            <SectionField
              name={"level"}
              link={getConfigurationLink("#instance_log_level")}
            >
              {fullInstance.log?.level}
            </SectionField>

            <SectionField
              title={fullInstance.log?.destination}
              name={"destination"}
              link={getConfigurationLink("#instance_log_destination")}
            >
              <Address value={fullInstance.log?.destination} />
            </SectionField>
            <SectionField
              name={"format"}
              link={getConfigurationLink("#instance_log_format")}
            >
              {fullInstance.log?.format}
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
      <Box padding={"17px"} borderBottom={border}>
        <Section title={commontContentTranslation.state}>
          <SectionFieldsContent>
            <SectionField
              title={fullInstance.picodataVersion}
              name={commontContentTranslation.picodataVersion}
            >
              <Address value={fullInstance.picodataVersion} />
            </SectionField>
            <SectionField
              withTitle={false}
              name={commontContentTranslation.currentState}
              link={getGlossaryLink("#state")}
            >
              <State value={instance.currentState} />
            </SectionField>
            <SectionField
              withTitle={false}
              name={commontContentTranslation.targetState}
              link={getGlossaryLink("#state")}
            >
              <State value={instance.targetState} />
            </SectionField>
          </SectionFieldsContent>
        </Section>
      </Box>
    </Root>
  );
};
