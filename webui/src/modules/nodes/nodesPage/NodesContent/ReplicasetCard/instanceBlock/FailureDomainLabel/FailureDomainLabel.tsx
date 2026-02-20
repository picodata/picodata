import React from "react";

import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { sortByString } from "shared/utils/string/sort";
import { Clippable } from "shared/ui/Clippable/Clippable";

import { formatFailDomain } from "../../../utils";

import { containerSx, Text } from "./StyledComponents";

type FailureDomainLabelProps = {
  failureDomain: {
    key: string;
    value: string;
  }[];
};

export const FailureDomainLabel: React.FC<FailureDomainLabelProps> = (
  props
) => {
  const { failureDomain } = props;

  return (
    <HiddenWrapper sx={containerSx}>
      {failureDomain
        .map(formatFailDomain)
        .sort((a, b) => sortByString(b, a)) // Ensure consistent display order for key=value pairs
        .map((domain, index) => {
          const isLastItem = index === failureDomain.length - 1;

          return (
            <React.Fragment key={index}>
              <Clippable text={domain} inline>
                <Text>
                  {domain}
                  {isLastItem ? "" : ";"}
                </Text>
              </Clippable>
            </React.Fragment>
          );
        })}
    </HiddenWrapper>
  );
};
