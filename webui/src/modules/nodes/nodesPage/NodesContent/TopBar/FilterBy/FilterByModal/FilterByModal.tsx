import React, { useCallback, useMemo } from "react";

import { Button } from "shared/ui/Button/Button";
import { PlusIcon } from "shared/icons/PlusIcon";
import { truthy } from "shared/utils/tsUtils";
import { getUniqueArrayBy } from "shared/utils/array/getUniqueArrayBy";
import { useTranslation } from "shared/intl";
import { PromptIcon } from "shared/components/Prompt/PromptIcon";

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

  const [
    keyValueFilters,
    addKeyValueFilter,
    updateKeyValueFilter,
    deleteFilter,
  ] = useKeyValues(values.domainValuesFilters);

  const { translation } = useTranslation();
  const modalTranslations = translation.pages.instances.filterBy.modal;

  const onApplyClick = useCallback(() => {
    const domainValuesFilters = keyValueFilters
      .map((filter) => {
        if (!filter.key || !filter.value?.length) return null;

        return {
          uniqueStr: `${filter.key}:${filter.value.join(",")}`,
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

  const notSelectedDomains = useMemo(() => {
    const keys = keyValueFilters.map((keyValue) => keyValue.key);

    return domains.filter((domain) => !keys.includes(domain.key));
  }, [domains, keyValueFilters]);

  const canAddMoreFilters = !!notSelectedDomains.length;

  return (
    <>
      <div className={styles.field}>
        <div className={styles.label}>
          {modalTranslations.failureDomainField.label}
          <PromptIcon>
            {modalTranslations.failureDomainField.promptText}
          </PromptIcon>
        </div>
        <div className={styles.scroll}>
          {keyValueFilters.map((filter, i) => {
            const currentDomains = domains.filter(
              (domain) => domain.key === filter.key
            );

            return (
              <DomainField
                key={filter.id}
                filter={filter}
                domains={
                  currentDomains.length
                    ? [...notSelectedDomains, ...currentDomains]
                    : notSelectedDomains
                }
                onDelete={
                  i === 0
                    ? undefined
                    : () => {
                        deleteFilter(filter.id);
                      }
                }
                updateKeyValueFilter={updateKeyValueFilter}
              />
            );
          })}
        </div>
      </div>
      {canAddMoreFilters && (
        <div className={styles.addFilter} onClick={addKeyValueFilter}>
          <PlusIcon />
        </div>
      )}
      <div className={styles.footer}>
        <Button size="small" className={styles.apply} onClick={onApplyClick}>
          {modalTranslations.ok}
        </Button>
        <Button
          size="small"
          className={styles.clear}
          theme="secondary"
          onClick={() =>
            onApply({
              domainValuesFilters: undefined,
            })
          }
        >
          {modalTranslations.clear}
        </Button>
      </div>
    </>
  );
};
