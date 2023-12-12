import React from "react";
import cn from "classnames";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";

import { formatFailDomain } from "../../../utils";

import styles from "./FailureDomainLabel.module.scss";

type FailureDomainLabelProps = {
  className?: string;
  failureDomain: {
    key: string;
    value: string;
  }[];
};

export const FailureDomainLabel: React.FC<FailureDomainLabelProps> = (
  props
) => {
  const { className, failureDomain } = props;

  return (
    <div className={cn(styles.container, className)}>
      {failureDomain.map((domain, index) => {
        return (
          <div key={index} className={styles.row}>
            <LinkSystemIcon width={16} height={16} />
            {formatFailDomain(domain)}
          </div>
        );
      })}
    </div>
  );
};
