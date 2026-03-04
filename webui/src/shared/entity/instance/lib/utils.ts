import { InstanceNodeType, InstanceType } from "../common";

export const sortInstances = <T extends InstanceType | InstanceNodeType>(
  instances: T[]
): T[] => {
  const resultInstances = [...instances];
  resultInstances.sort((instanceA, instanceB) =>
    instanceA.name < instanceB.name ? -1 : 1
  );
  return resultInstances;
};
