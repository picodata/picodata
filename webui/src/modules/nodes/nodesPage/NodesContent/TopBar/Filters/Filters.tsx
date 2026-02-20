import React from "react";
import { SxProps } from "@mui/system/styleFunctionSx";

import { Button } from "shared/ui/Button/Button";
import { Tag } from "shared/ui/Tag/Tag";
import { useTranslation } from "shared/intl";

import { TFilterByValue } from "../FilterBy/config";
import { formatFailDomain } from "../../utils";

import { Root } from "./StyledComponents";

type FiltersProps = {
  filterByValue: TFilterByValue;
  sx?: SxProps;
  setFilterByValue: (value?: TFilterByValue) => void;
};

export const Filters: React.FC<FiltersProps> = (props) => {
  const { filterByValue, sx, setFilterByValue } = props;

  const { translation } = useTranslation();
  const filtersTranslations = translation.pages.instances.filters;

  const deleteItem = (filterForDelete: { key: string; value: string[] }) => {
    setFilterByValue({
      ...filterByValue,
      domain: filterByValue.domain?.filter(
        (domain) =>
          !(
            domain.key === filterForDelete.key &&
            domain.value === filterForDelete.value
          )
      ),
    });
  };

  if (!filterByValue.domain?.length) return null;

  return (
    <Root sx={sx}>
      <Button
        theme="secondary"
        size="extraSmall"
        onClick={() => setFilterByValue()}
      >
        {filtersTranslations.clearAll}
      </Button>
      {filterByValue.domain.map((domain) => {
        const label = formatFailDomain(domain);

        return (
          <Tag
            key={label}
            theme="secondary"
            size="extraSmall"
            closeIcon
            onClose={() => deleteItem(domain)}
          >
            {label}
          </Tag>
        );
      })}
    </Root>
  );
};
