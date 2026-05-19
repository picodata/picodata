import { OverridableComponent } from "@mui/material/OverridableComponent";
import { SvgIconTypeMap } from "@mui/material";

type Value = string | number | boolean;

export enum EditableValueStatusEnum {
  Done = "Done",
  Tag = "Tag",
  Expression = "Expression",
  Value = "Value",
}

export enum ExpressionEnum {
  Is = "Is",
  IsOneOf = "IsOneOf",
  IsNotOneOf = "IsNotOneOf",
}

export interface Expression {
  type: ExpressionEnum;
  label: string;
  description: string;
}

export type TagOption = {
  value: Value;
  label: string;
};

export type Tag = {
  key: string;
  label: string;
  icon?: OverridableComponent<SvgIconTypeMap>;
  options: TagOption[];
};

export type FilterValue = {
  id: string;
  tagKey: string;
  expression: Expression;
  value: Value | Value[];
};

export type EditableFilterValue = Partial<FilterValue> &
  Pick<FilterValue, "id"> & {
    status: EditableValueStatusEnum;
  };

export type FilterProps = {
  tags: Tag[];
  value: FilterValue[];
  onChange: (value: FilterValue[]) => void;
};
