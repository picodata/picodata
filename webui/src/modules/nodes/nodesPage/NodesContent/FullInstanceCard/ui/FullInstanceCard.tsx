import { Box, styled, SxProps, Theme } from "@mui/material";
import { useEffect, useMemo } from "react";

import { useFullInstance } from "shared/entity/instance/useQuery";
import { Modal } from "shared/ui/AltModal";
import { InstanceType } from "shared/entity/instance";
import { Progress } from "shared/ui/Progress";
import { ScrollBox } from "shared/ui/ScrollBox";

import { useTabs } from "../model";

import { Tabs } from "./Tabs";
import { Title } from "./Title";
import { Fork } from "./Fork";
import { ErrorMessage } from "./ErrorMessage";

const Root = styled(Box)({
  overflow: "hidden",
  width: "100%",
  height: "100%",
  display: "grid",
  gridTemplateRows: "min-content 1fr",
});

const modalSx: SxProps<Theme> = (theme) => ({
  width: "90vw",
  height: 670,
  [theme.breakpoints.up("md")]: {
    width: 800,
  },
});

type FullInstanceCardProps = {
  instance: InstanceType;
  instances: InstanceType[];
  onClose: () => void;
};
export const FullInstanceCard = ({
  instances,
  instance,
  onClose,
}: FullInstanceCardProps) => {
  const {
    data: fullInstance,
    isLoading,
    refetch,
    isError,
  } = useFullInstance(instance.uuid);
  const [tab, setTab] = useTabs();
  const replicaDisabled = useMemo(() => {
    return (
      !fullInstance ||
      !fullInstance.replication ||
      Object.keys(fullInstance.replication).length < 2
    );
  }, [fullInstance]);
  useEffect(() => {
    refetch();
  }, [instance.uuid, refetch]);

  return (
    <Modal
      title={<Title instance={instance} />}
      open={true}
      onClose={onClose}
      sx={modalSx}
    >
      {isLoading ? (
        <Progress />
      ) : isError ? (
        <ErrorMessage />
      ) : (
        <Root>
          <ScrollBox>
            <Tabs tab={tab} setTab={setTab} replicaDisabled={replicaDisabled} />
          </ScrollBox>
          <ScrollBox>
            {fullInstance && (
              <Fork
                tab={tab}
                fullInstance={fullInstance}
                instance={instance}
                instances={instances}
              />
            )}
          </ScrollBox>
        </Root>
      )}
    </Modal>
  );
};
