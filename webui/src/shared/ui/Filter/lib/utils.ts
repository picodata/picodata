import {
  EditableFilterValue,
  EditableValueStatusEnum,
  FilterValue,
  TagOption,
} from "../model";
import { TIntlContext } from "../../../intl";

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
export const getYesNoOptions = (
  translation: TIntlContext["translation"]
): TagOption[] => {
  return [
    { value: true, label: translation.common.yes },
    { value: false, label: translation.common.no },
  ];
};
