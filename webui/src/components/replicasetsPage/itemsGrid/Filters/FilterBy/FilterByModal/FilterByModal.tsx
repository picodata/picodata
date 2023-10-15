import React from "react";

import styles from "./FilterByModal.module.scss";
import { Button } from "components/shared/ui/Button/Button";
import { PlusIcon } from "components/icons/PlusIcon";
import { TKeyValueFilter } from "./types";
import { useKeyValues } from "./hooks";
import { FilterKey } from "./FilterKey/FilterKey";

export type FilterByModalProps = {
  keys: string[];
  keyValueFilters?: TKeyValueFilter[];
};

export const FilterByModal: React.FC<FilterByModalProps> = (props) => {
  const { keyValueFilters: propsKeyValueFilters, keys } = props;

  const [keyValueFilters, addKeyValueFilter, updateKeyValueFilter] =
    useKeyValues(propsKeyValueFilters);

  return (
    <div className={styles.container}>
      <div className={styles.field}>
        <div className={styles.label}>Name</div>
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
              <div className={styles.keyValueField}>
                <div className={styles.label}>Value</div>
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
