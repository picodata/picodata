import { ServerInstanceType } from "./types";

export const mapInstanceToClient = <
  T extends Pick<ServerInstanceType, "failureDomain">
>(
  instance: T
) => {
  return {
    ...instance,
    failureDomain: Object.entries(instance.failureDomain).map(
      ([key, value]) => ({
        key,
        value,
      })
    ),
  };
};
