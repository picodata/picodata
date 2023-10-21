import React, { useEffect, useMemo } from "react";
import cn from "classnames";

import { Select, TOption } from "shared/ui/Select/Select";

import { FilterKey } from "./FilterKey/FilterKey";
import { TKeyValueFilter } from "./types";
import { useKeysValuesData } from "./hooks";

import styles from "./DomainField.module.scss";

export type DomainFieldProps = {
  className?: string;
  filter: TKeyValueFilter;
  domains: Array<{ key: string; value: string }>;
  updateKeyValueFilter: (
    id: number,
    updData: Partial<Omit<TKeyValueFilter, "id">>
  ) => void;
};

export const DomainField: React.FC<DomainFieldProps> = (props) => {
  const { className, filter, domains, updateKeyValueFilter } = props;

  const { keys, values } = useKeysValuesData(domains, filter);

  useEffect(() => {
    if (filter.value && !values.includes(filter.value)) {
      updateKeyValueFilter(filter.id, { value: "" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [values, filter.value, filter.id]);

  const domainValuesOptions = useMemo(
    () =>
      values.map((name) => ({
        label: name,
        value: name,
      })),
    [values]
  );

  return (
    <div className={className}>
      <div className={styles.keyValueField}>
        <div className={styles.label}>Key</div>
        <div className={styles.keys}>
          {keys.map((key) => {
            const isActive = key === filter.key;

            return (
              <FilterKey
                key={key}
                isActive={isActive}
                onClick={() => updateKeyValueFilter(filter.id, { key })}
              >
                {key}
              </FilterKey>
            );
          })}
        </div>
      </div>
      <div className={cn(styles.keyValueField, styles.keyValueFieldCenter)}>
        <div className={styles.label}>Value</div>
        <Select
          options={domainValuesOptions}
          classNames={{ container: () => styles.valueSelect }}
          isMulti={false}
          value={
            domainValuesOptions.find((o) => o.value === filter.value) ?? null
          }
          onChange={(newOption) => {
            if (!newOption || Array.isArray(newOption)) return;

            const option = newOption as TOption;

            updateKeyValueFilter(filter.id, { value: option.value });
          }}
        />
      </div>
    </div>
  );
};
