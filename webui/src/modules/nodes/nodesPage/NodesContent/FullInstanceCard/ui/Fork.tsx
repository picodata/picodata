import { FullInstance, InstanceType } from "shared/entity/instance";

import { CommonContent } from "./CommonContent";
import { Vinyl } from "./Vinyl";
import { Replication } from "./Replication";

type ForkProps = {
  tab: number;
  instances: InstanceType[];
  instance: InstanceType;
  fullInstance: FullInstance;
};
export const Fork = ({ tab, instances, ...props }: ForkProps) => {
  switch (tab) {
    case 0: {
      return <CommonContent {...props} />;
    }
    case 1: {
      return (
        <Vinyl
          vinyl={props.fullInstance.vinyl}
          memtx={props.fullInstance.memtx}
        />
      );
    }
    case 2: {
      return (
        <Replication
          instances={instances}
          replications={props.fullInstance.replication || {}}
          currentInstanceId={props.instance.uuid}
        />
      );
    }
    default:
      return <></>;
  }
};
