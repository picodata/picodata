import React from "react";
import cn from "classnames";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

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
    <div className={cn(styles.container, className)}>
      <LinkSystemIcon width={16} height={16} className={styles.icon} />
      <HiddenWrapper>{address}</HiddenWrapper>
    </div>
  );
};
