import React, { useMemo } from "react";

import { TIntlContext, useTranslation } from "shared/intl";
import { TextInFrame } from "shared/ui/typography/TextInFrame/TextInFrame";

import styles from "./NetworkState.module.scss";

type NetworkStateProps = {
  state: "Online" | "Offline" | "Expelled";
};

/** Which label to show for what state */
const translationLabel = {
  Online: "online",
  Offline: "offline",
  Expelled: "unknown",
} satisfies {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  [_ in NetworkStateProps["state"]]: keyof TIntlContext["translation"]["components"]["networkState"]["label"];
};

export const NetworkState: React.FC<NetworkStateProps> = (props) => {
  const { state } = props;

  const { translation } = useTranslation();
  const networkStateTranslations = translation.components.networkState;
  const isOffline = useMemo(() => state === "Offline", [state]);

  const getLabel = () =>
    networkStateTranslations.label[translationLabel[state]];

  return (
    <TextInFrame>
      {isOffline && <div className={styles.asideIndicator} />}
      {getLabel()}
    </TextInFrame>
  );
};
