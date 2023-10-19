import React from "react";
import { TFilterByValue } from "./config";
import { FunnelIcon } from "components/icons/FunnelIcon";

import { ButtonModal } from "components/shared/ui/ButtonModal/ButtonModal";
import { FilterByModal } from "./FilterByModal/FilterByModal";

import { RootState } from "store";
import { useSelector } from "react-redux";
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
