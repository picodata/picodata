import React from "react";

import { useTranslation } from "shared/intl";
import { TextInFrame } from "shared/ui/typography/TextInFrame/TextInFrame";

type NetworkStateProps = {
  state: "Online" | "Offline";
};

export const NetworkState: React.FC<NetworkStateProps> = (props) => {
  const { state } = props;

  const { translation } = useTranslation();
  const networkStateTranslations = translation.components.networkState;

  const getLabel = () => {
    if (state === "Online") {
      return networkStateTranslations.label.online;
    }
    if (state === "Offline") {
      return networkStateTranslations.label.offline;
    }

    return networkStateTranslations.label.unknown;
  };

  return <TextInFrame>{getLabel()}</TextInFrame>;
};
