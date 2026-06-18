import { Box, styled } from "@mui/material";

import { InstanceMemtx, InstanceVinyl } from "shared/entity/instance";
import { getConfigurationLink } from "shared/config";

import { Section, SectionField, SectionFieldsContent } from "../common";

const VinylSectionFieldsContent = styled(SectionFieldsContent)({
  gridTemplateColumns: "360px auto",
});

const Root = styled(Box)({
  padding: 20,
  display: "flex",
  flexDirection: "column",
  gap: 10,
});

type VinylProps = {
  vinyl?: InstanceVinyl;
  memtx?: InstanceMemtx;
};
export const Vinyl = ({ vinyl, memtx }: VinylProps) => {
  return (
    <Root>
      {memtx ? (
        <Section title={"memtx"}>
          <VinylSectionFieldsContent>
            <SectionField
              name={"max_tuple_size"}
              link={getConfigurationLink("#instance_memtx_max_tuple_size")}
            >
              {memtx.maxTupleSize}
            </SectionField>
            <SectionField
              name={"memory"}
              link={getConfigurationLink("#instance_memtx_memory")}
            >
              {memtx.memory}
            </SectionField>
            <SectionField
              name={"system_memory"}
              link={getConfigurationLink("#instance_memtx_system_memory")}
            >
              {memtx.systemMemory}
            </SectionField>
          </VinylSectionFieldsContent>
        </Section>
      ) : null}
      {vinyl ? (
        <Section title={"vinyl"}>
          <VinylSectionFieldsContent>
            <SectionField
              name={"bloom_fpr"}
              link={getConfigurationLink("#instance_vinyl_bloom_fpr")}
            >
              {vinyl.bloomFpr}
            </SectionField>
            <SectionField
              name={"cache"}
              link={getConfigurationLink("#instance_vinyl_cache")}
            >
              {vinyl.cache}
            </SectionField>
            <SectionField
              name={"max_tuple_size"}
              link={getConfigurationLink("#instance_vinyl_max_tuple_size")}
            >
              {vinyl.maxTupleSize}
            </SectionField>
            <SectionField
              name={"memory"}
              link={getConfigurationLink("#instance_vinyl_memory")}
            >
              {vinyl.memory}
            </SectionField>
            <SectionField
              name={"page_size"}
              link={getConfigurationLink("#instance_vinyl_page_size")}
            >
              {vinyl.pageSize}
            </SectionField>
            <SectionField
              name={"range_size"}
              link={getConfigurationLink("#instance_vinyl_range_size")}
            >
              {vinyl.rangeSize}
            </SectionField>
            <SectionField
              name={"read_threads"}
              link={getConfigurationLink("#instance_vinyl_read_threads")}
            >
              {vinyl.readThreads}
            </SectionField>
            <SectionField
              name={"run_count_per_level"}
              link={getConfigurationLink("#instance_vinyl_run_count_per_level")}
            >
              {vinyl.runCountPerLevel}
            </SectionField>
            <SectionField
              name={"run_size_ratio"}
              link={getConfigurationLink("#instance_vinyl_run_size_ratio")}
            >
              {vinyl.runSizeRatio}
            </SectionField>
            <SectionField
              name={"timeout"}
              link={getConfigurationLink("#instance_vinyl_timeout")}
            >
              {vinyl.timeout}
            </SectionField>
            <SectionField
              name={"write_threads"}
              link={getConfigurationLink("#instance_vinyl_write_threads")}
            >
              {vinyl.writeThreads}
            </SectionField>
          </VinylSectionFieldsContent>
        </Section>
      ) : null}
    </Root>
  );
};
