import {
  Filter as SharedFilter,
  FilterProps,
  FilterValue,
} from "shared/ui/Filter";

export type FilterModuleProps = {
  filterValue: FilterValue[];
  onFilterValueChange: (filterValue: FilterValue[]) => void;
  filterTags: FilterProps["tags"];
};
export const Filter = ({
  onFilterValueChange,
  filterValue,
  filterTags,
}: FilterModuleProps) => {
  return (
    <SharedFilter
      tags={filterTags}
      value={filterValue}
      onChange={onFilterValueChange}
    />
  );
};
