import React, { useCallback } from "react";

import { Button } from "shared/ui/Button/Button";
import { PlusIcon } from "shared/icons/PlusIcon";
import { truthy } from "shared/utils/tsUtils";
import { getUniqueArrayBy } from "shared/utils/array/getUniqueArrayBy";

import { TKeyValueFilter } from "./DomainField/types";
import { useKeyValues } from "./hooks";
import { DomainField, DomainFieldProps } from "./DomainField/DomainField";

import styles from "./FilterByModal.module.scss";

export type Values = {
  domainValuesFilters?: Omit<TKeyValueFilter, "id">[];
};

export type FilterByModalProps = {
  domains: DomainFieldProps["domains"];
  values: Values;
  onApply: (values: Values) => void;
};

export const FilterByModal: React.FC<FilterByModalProps> = (props) => {
  const { domains, values, onApply } = props;

  const [keyValueFilters, addKeyValueFilter, updateKeyValueFilter] =
    useKeyValues(values.domainValuesFilters);

  const onApplyClick = useCallback(() => {
    const domainValuesFilters = keyValueFilters
      .map((filter) => {
        if (!filter.key || !filter.value) return null;

        return {
          uniqueStr: `${filter.key}:${filter.value}`,
          key: filter.key,
          value: filter.value,
        };
      })
      .filter(truthy);

    const uniqueDomainValuesFilters = getUniqueArrayBy(
      domainValuesFilters,
      "uniqueStr"
    ).map((filter) => ({
      key: filter.key,
      value: filter.value,
    }));

    onApply({
      domainValuesFilters: uniqueDomainValuesFilters.length
        ? uniqueDomainValuesFilters
        : undefined,
    });
  }, [onApply, keyValueFilters]);

  return (
    <div className={styles.container}>
      <div className={styles.field}>
        <div className={styles.label}>Failure domain</div>
        {keyValueFilters.map((filter) => {
          return (
            <DomainField
              key={filter.id}
              filter={filter}
              className={styles.keyValueForm}
              domains={domains}
              updateKeyValueFilter={updateKeyValueFilter}
            />
          );
        })}
      </div>
      <Button
        size="extraSmall"
        theme="secondary"
        leftIcon={<PlusIcon />}
        onClick={addKeyValueFilter}
      >
        Add key value pair
      </Button>
      <div className={styles.footer}>
        <Button onClick={onApplyClick}>Apply filter</Button>
      </div>
    </div>
  );
};
