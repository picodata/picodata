import React from "react";
import cn from "classnames";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";
import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

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
    <HiddenWrapper className={cn(styles.container, className)}>
      {failureDomain.map((domain, index) => {
        const isLastItem = index === failureDomain.length - 1;

        return (
          <React.Fragment key={index}>
            <LinkSystemIcon width={16} height={16} className={styles.icon} />
            <span className={styles.text}>
              {formatFailDomain(domain)}
              {isLastItem ? "" : ";"}
            </span>
          </React.Fragment>
        );
      })}
    </HiddenWrapper>
  );
};
