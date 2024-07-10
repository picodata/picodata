import React, { useState } from "react";

import { useTiers } from "shared/entity/tier";
import { Modal } from "shared/ui/Modal/Modal";
import { CloseIcon } from "shared/icons/CloseIcon";
import { FilterByButton } from "shared/components/buttons/FilterByButton/FilterByButton";
import { useTranslation } from "shared/intl";

import { TFilterByValue } from "./config";
import { FilterByModal } from "./FilterByModal/FilterByModal";
import { useInstancesFiltersData } from "./hooks";

import styles from "./FilterBy.module.scss";

export type FilterByProps = {
  filterByValue?: TFilterByValue;
  setFilterByValue: (value?: TFilterByValue) => void;
};

export const FilterBy: React.FC<FilterByProps> = (props) => {
  const { filterByValue, setFilterByValue } = props;
  const { data } = useTiers();
  const [isOpen, setIsOpen] = useState(false);

  const { domains } = useInstancesFiltersData(data?.instances ?? []);

  const { translation } = useTranslation();
  const filterByTranslation = translation.pages.instances.filterBy;

  const renderModal = () => {
    if (!isOpen) {
      return null;
    }

    return (
      <Modal bodyClassName={styles.body}>
        <>
          <div className={styles.titleWrapper}>
            <span className={styles.titleText}>
              {filterByTranslation.modal.title}
            </span>
            <div
              className={styles.close}
              onClick={(event) => {
                event.stopPropagation();
                setIsOpen(false);
              }}
            >
              <CloseIcon />
            </div>
          </div>
          <FilterByModal
            domains={domains}
            values={{
              domainValuesFilters: filterByValue?.domain,
            }}
            onApply={(values) => {
              setFilterByValue({
                domain: values.domainValuesFilters,
              });
              setIsOpen(false);
            }}
          />
        </>
      </Modal>
    );
  };

  return (
    <>
      <FilterByButton onClick={() => setIsOpen(true)} />
      {renderModal()}
    </>
  );
};
