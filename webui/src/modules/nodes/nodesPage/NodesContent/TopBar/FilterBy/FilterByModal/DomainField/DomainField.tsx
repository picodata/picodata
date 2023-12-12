import React, { useEffect, useMemo } from "react";
import cn from "classnames";

import { Select } from "shared/ui/Select/Select";
import { isArrayContainsOtherArray } from "shared/utils/array/isArrayContainsOtherArray";
import { TrashIcon } from "shared/icons/TrashIcon";

import { TKeyValueFilter } from "./types";
import { useKeysValuesData } from "./hooks";

import styles from "./DomainField.module.scss";

export type DomainFieldProps = {
  className?: string;
  onDelete?: () => void;
  filter: TKeyValueFilter;
  domains: Array<{ key: string; value: string }>;
  updateKeyValueFilter: (
    id: number,
    updData: Partial<Omit<TKeyValueFilter, "id">>
  ) => void;
};

export const DomainField: React.FC<DomainFieldProps> = (props) => {
  const { className, filter, domains, updateKeyValueFilter, onDelete } = props;

  const { keys, values } = useKeysValuesData(domains, filter);

  useEffect(() => {
    if (
      filter.value &&
      filter.value.length &&
      !isArrayContainsOtherArray(values, filter.value)
    ) {
      updateKeyValueFilter(filter.id, { value: [] });
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

  const domainValuesValue = useMemo(() => {
    return domainValuesOptions.filter((o) => filter.value?.includes(o.value));
  }, [domainValuesOptions, filter.value]);

  return (
    <div className={cn(styles.container, className)}>
      <div className={styles.keyValueField}>
        <Select
          options={keys.map((key) => ({ label: key, value: key }))}
          classNames={{ container: () => styles.valueSelect }}
          isMulti={false}
          placeholder="Key"
          value={
            filter.key ? { label: filter.key, value: filter.key } : undefined
          }
          onChange={(newOptions) => {
            if (!newOptions || Array.isArray(newOptions)) return;

            const newValue = newOptions as {
              label: string;
              value: string;
            };

            updateKeyValueFilter(filter.id, {
              key: newValue.value,
            });
          }}
        />
      </div>
      <div className={cn(styles.keyValueField, styles.keyValueFieldCenter)}>
        <Select
          options={domainValuesOptions}
          classNames={{ container: () => styles.valueSelect }}
          isMulti
          placeholder="Value"
          value={domainValuesValue}
          onChange={(newOptions) => {
            if (!newOptions || !Array.isArray(newOptions)) return;

            updateKeyValueFilter(filter.id, {
              value: newOptions.map((o) => o.value),
            });
          }}
        />
      </div>
      {!!onDelete && (
        <div className={styles.deleteContainer}>
          <div className={styles.delete} onClick={onDelete}>
            <TrashIcon />
          </div>
        </div>
      )}
    </div>
  );
};
