import {
  EditableFilterValue,
  EditableValueStatusEnum,
  FilterValue,
} from "../model";

export const getEditableFilterValue = (
  filterValue: FilterValue[]
): EditableFilterValue[] => {
  return filterValue.map((valueItem) => ({
    ...valueItem,
    status: EditableValueStatusEnum.Done,
  }));
};
export const getFilterValueByEditableFilterValue = (
  filterValue: EditableFilterValue[]
): FilterValue[] => {
  return filterValue.map(
    ({ status, ...editableFilterValue }) => editableFilterValue as FilterValue
  );
};
