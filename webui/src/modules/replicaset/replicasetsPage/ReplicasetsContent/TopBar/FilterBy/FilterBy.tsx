import React from "react";
import { useSelector } from "react-redux";

import { FunnelIcon } from "shared/icons/FunnelIcon";
import { ButtonModal } from "shared/ui/ButtonModal/ButtonModal";
import { RootState } from "store";

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
  const instances = useSelector((state: RootState) => state.cluster.instances);

  const { domains } = useInstancesFiltersData(instances);

  return (
    <ButtonModal
      buttonProps={{
        size: "normal",
        rightIcon: <FunnelIcon />,
        children: "Filter by",
      }}
      modalProps={{
        title: "Filter by",
        bodyClassName: styles.modal,
      }}
    >
      {({ onClose }) => (
        <FilterByModal
          domains={domains}
          values={{
            domainValuesFilters: filterByValue?.domain,
          }}
          onApply={(values) => {
            setFilterByValue({
              domain: values.domainValuesFilters,
            });
            onClose();
          }}
        />
      )}
    </ButtonModal>
  );
};
