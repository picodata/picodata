import { ExpressionEnum, Expression } from "../model";

export const expressionOptions: Expression[] = [
  {
    type: ExpressionEnum.Is,
    label: "is",
    description: "==",
  },
  {
    type: ExpressionEnum.IsNotOneOf,
    label: "is not one of",
    description: "!=",
  },
  {
    type: ExpressionEnum.IsOneOf,
    label: "is one of",
    description: "|",
  },
];

export const SEARCH_TEXT_KEY = "searchText";
