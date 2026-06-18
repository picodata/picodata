import { useSearchParams } from "react-router-dom";
import { useCallback, useMemo } from "react";

import { InstanceReplication } from "../../../../../../shared/entity/instance";

import { FULL_INSTANCE_CARD_SEARCH_PARAMS_KEY } from "./constant";

export const useOpenFullInstanceCard = () => {
  const [sp, setSp] = useSearchParams();
  const openFullInstanceCard = useCallback(
    (id: string) => {
      setSp((_sp) => {
        _sp.set(FULL_INSTANCE_CARD_SEARCH_PARAMS_KEY, id);
        return _sp;
      });
    },
    [setSp]
  );
  const closeFullInstanceCard = useCallback(() => {
    setSp((_sp) => {
      _sp.delete(FULL_INSTANCE_CARD_SEARCH_PARAMS_KEY);
      return _sp;
    });
  }, [setSp]);

  const fullInstanceId = sp.get(FULL_INSTANCE_CARD_SEARCH_PARAMS_KEY);

  return useMemo(
    () => ({
      openFullInstanceCard,
      closeFullInstanceCard,
      fullInstanceId,
    }),
    [openFullInstanceCard, closeFullInstanceCard, fullInstanceId]
  );
};

export const useReplicationInstances = (
  replications: Record<number, InstanceReplication>,
  currentInstanceId: string
) => {
  return useMemo(() => {
    let currentInstanceInner: InstanceReplication | null = null;
    const otherInstancesInner: InstanceReplication[] = [];
    for (const key in replications) {
      const current = replications[Number(key)];
      if (current.uuid === currentInstanceId) {
        currentInstanceInner = current;
      } else {
        otherInstancesInner.push(current);
      }
    }
    return [currentInstanceInner, otherInstancesInner] as const;
  }, [replications, currentInstanceId]);
};
