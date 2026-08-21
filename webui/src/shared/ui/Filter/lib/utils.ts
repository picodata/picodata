import {
  EditableFilterValue,
  EditableValueStatusEnum,
  FilterValue,
  TagOption,
  Value,
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

export const getYesNoLabel = (
  value: boolean,
  translation: TIntlContext["translation"]
) => (value ? translation.common.yes : translation.common.no);

export const getYesNoOptions = (
  translation: TIntlContext["translation"]
): TagOption[] => {
  return [
    { value: true, label: getYesNoLabel(true, translation) },
    { value: false, label: getYesNoLabel(false, translation) },
  ];
};

export const formatFilterValue = (
  value: Value | Value[] | undefined,
  translation: TIntlContext["translation"]
) => {
  switch (true) {
    case value === undefined: {
      return "-";
    }
    case Array.isArray(value): {
      return (value as Value[])
        .map((valueItem) =>
          typeof valueItem === "boolean"
            ? getYesNoLabel(valueItem, translation)
            : valueItem
        )
        .join(" | ");
    }
    case typeof value === "boolean": {
      return getYesNoLabel(value as boolean, translation);
    }
    default:
      return String(value);
  }
};
