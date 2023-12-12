import React from "react";
import cn from "classnames";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";

import styles from "./IpAddressLabel.module.scss";

type IpAddressLabelProps = {
  className?: string;
  address: string;
};

export const IpAddressLabel: React.FC<IpAddressLabelProps> = (props) => {
  const { className, address } = props;

  if (!address) return "-";

  return (
    <div className={cn(styles.container, className)}>
      <LinkSystemIcon width={16} height={16} />
      {address}
    </div>
  );
};
