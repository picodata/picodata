import React, { useState } from "react";

import { useTiers } from "shared/entity/tier";
import { Modal } from "shared/ui/Modal/Modal";
import { CloseIcon } from "shared/icons/CloseIcon";
import { FunnelIcon } from "shared/icons/FunnelIcon";
import { Button } from "shared/ui/Button/Button";

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

  const renderModal = () => {
    if (!isOpen) {
      return null;
    }

    return (
      <Modal bodyClassName={styles.body}>
        <>
          <div className={styles.titleWrapper}>
            <span className={styles.titleText}>Filter</span>
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
      <Button
        size="normal"
        rightIcon={<FunnelIcon />}
        onClick={() => setIsOpen(true)}
      >
        Filter by
      </Button>
      {renderModal()}
    </>
  );
};
