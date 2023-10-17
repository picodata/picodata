import React, { useState } from "react";
import cn from "classnames";

import { Button } from "components/shared/ui/Button/Button";
import { PlusIcon } from "components/icons/PlusIcon";
import { TKeyValueFilter } from "./types";
import { useKeyValues } from "./hooks";
import { FilterKey } from "./FilterKey/FilterKey";
import { Select } from "components/shared/ui/Select/Select";

import styles from "./FilterByModal.module.scss";

export type FilterByModalProps = {
  keys: string[];
  names: string[];
  keyValueFilters?: TKeyValueFilter[];
};

export const FilterByModal: React.FC<FilterByModalProps> = (props) => {
  const { names, keyValueFilters: propsKeyValueFilters, keys } = props;

  const [selectedNameOption, setSelectedNameOption] = useState<{
    label: string;
    value: string;
  }>();
  const [keyValueFilters, addKeyValueFilter, updateKeyValueFilter] =
    useKeyValues(propsKeyValueFilters);

  return (
    <div className={styles.container}>
      <div className={styles.field}>
        <div className={styles.label}>Name</div>
        <Select
          options={names.map((name) => ({ label: name, value: name }))}
          classNames={{ container: () => styles.nameSelect }}
          value={selectedNameOption}
          isMulti={false}
          onChange={(newOption) => {
            if (!newOption) {
              setSelectedNameOption(undefined);
              return;
            }

            if (Array.isArray(newOption)) return;

            const option = newOption as { label: string; value: string };

            setSelectedNameOption(option);
          }}
        />
      </div>
      <div className={styles.field}>
        <div className={styles.label}>Failure domain</div>
        {keyValueFilters.map((filter) => {
          return (
            <div key={filter.id} className={styles.keyValueForm}>
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
              <div
                className={cn(styles.keyValueField, styles.keyValueFieldCenter)}
              >
                <div className={styles.label}>Value</div>
                <Select
                  options={[
                    { label: "msk-1", value: 1 },
                    { label: "spb-3", value: 2 },
                  ]}
                  classNames={{ container: () => styles.valueSelect }}
                />
              </div>
            </div>
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
        <Button>Apply filter</Button>
      </div>
    </div>
  );
};
