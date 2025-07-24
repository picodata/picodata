import React from "react";
import cn from "classnames";

import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";
import { Clippable } from "shared/ui/Clippable/Clippable";

import styles from "./IpAddressLabel.module.scss";

type IpAddressLabelProps = {
  className?: string;
  address: string;
};

export const IpAddressLabel: React.FC<IpAddressLabelProps> = (props) => {
  const { className, address } = props;

  const { translation } = useTranslation();

  if (!address)
    return <InfoNoData text={translation.components.infoNoData.label} />;

  return (
    <Clippable className={cn(styles.container, className)} text={address}>
      {address}
    </Clippable>
  );
};
