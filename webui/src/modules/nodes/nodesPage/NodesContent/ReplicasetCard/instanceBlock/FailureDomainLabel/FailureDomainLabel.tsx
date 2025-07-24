import React from "react";
import cn from "classnames";

import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { sortByString } from "shared/utils/string/sort";
import { Clippable } from "shared/ui/Clippable/Clippable";

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
      {failureDomain
        .map(formatFailDomain)
        .sort((a, b) => sortByString(b, a)) // Ensure consistent display order for key=value pairs
        .map((domain, index) => {
          const isLastItem = index === failureDomain.length - 1;

          return (
            <React.Fragment key={index}>
              <Clippable text={domain} inline>
                <span className={styles.text}>
                  {domain}
                  {isLastItem ? "" : ";"}
                </span>
              </Clippable>
            </React.Fragment>
          );
        })}
    </HiddenWrapper>
  );
};
