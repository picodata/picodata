import { useMemo } from "react";

import {
  EditableFilterValue,
  EditableValueStatusEnum,
  Expression,
  ExpressionEnum,
  Tag,
  TagOption,
} from "../model";
import { SEARCH_TEXT_KEY } from "../lib";

import { ExpressionAutoComplete } from "./ExpressionAutoComplete";
import {
  FilterValueContainer,
  FilterValuePart,
  RestFilterValuePart,
} from "./common";
import { ValueAutoComplete } from "./ValueAutoComplete";
import { TagAutoComplete } from "./TagAutoComplete";
import { MultipleValueItem, MultipleValues } from "./common/MultipleValueItem";
import { SearchTextField } from "./SearchTextField";

type FilterValueFormProps = {
  value: EditableFilterValue;
  tags: Tag[];
  onChange: (filterValue: EditableFilterValue) => void;
  onDelete: (id: string) => void;
};
export const FilterValueForm = ({
  value: itemValue,
  onChange,
  onDelete,
  tags,
}: FilterValueFormProps) => {
  const { value, expression, tagKey, status, id } = itemValue;
  const tagOptions = useMemo(
    () => tags.find(({ key }) => key === tagKey)?.options || [],
    [tags, tagKey]
  );
  const resultValue = useMemo(() => {
    if (Array.isArray(value)) {
      return tagOptions.filter(({ value: _value }) => value.includes(_value));
    }
    if (expression?.type === ExpressionEnum.IsOneOf) {
      return [];
    }
    return tagOptions.find(({ value: _value }) => _value === value) || null;
  }, [tagOptions, value, expression]);

  const tagChangeHandler = (_tag: Tag | null) => {
    if (_tag) {
      onChange({
        ...itemValue,
        tagKey: _tag.key,
        status: EditableValueStatusEnum.Expression,
      });
    } else {
      onDelete(id);
    }
  };
  const expressionChangeHandler = (_expression: Expression | null) => {
    if (_expression) {
      onChange({
        ...itemValue,
        expression: _expression as Expression,
        status: EditableValueStatusEnum.Value,
      });
    } else {
      onDelete(id);
    }
  };
  const valueChangeHandler = (tagOption: TagOption | TagOption[] | null) => {
    if (tagOption) {
      onChange({
        ...itemValue,
        status: Array.isArray(tagOption)
          ? EditableValueStatusEnum.Value
          : EditableValueStatusEnum.Done,
        value: Array.isArray(tagOption)
          ? tagOption.map(({ value: _value }) => _value)
          : tagOption.value,
      });
    } else {
      onDelete(id);
    }
  };
  const expressionBlurHandler = () => {
    onChange({
      ...itemValue,
      status: EditableValueStatusEnum.Tag,
      expression: undefined,
    });
  };
  const valueBlurHandler = () => {
    onChange({
      ...itemValue,
      ...(Array.isArray(itemValue.value) && itemValue.value.length
        ? {
            status: EditableValueStatusEnum.Done,
          }
        : {
            status: EditableValueStatusEnum.Expression,
            expression: undefined,
          }),
    });
  };

  const searchTextChangeHandler = (_value: string) => {
    if (_value) {
      onChange({
        ...itemValue,
        status: EditableValueStatusEnum.Done,
        value: _value,
      });
    } else {
      onDelete(id);
    }
  };

  if (tagKey === SEARCH_TEXT_KEY) {
    return (
      <SearchTextField
        filterValue={itemValue}
        onBlurChange={searchTextChangeHandler}
      />
    );
  }

  let expressionPart = null;
  if (status === EditableValueStatusEnum.Expression) {
    expressionPart = (
      <FilterValuePart>
        <ExpressionAutoComplete
          onChange={(_, _value) => expressionChangeHandler(_value)}
          onBlur={expressionBlurHandler}
        />
      </FilterValuePart>
    );
  } else if (status === EditableValueStatusEnum.Value) {
    expressionPart = (
      <RestFilterValuePart>{expression?.description}</RestFilterValuePart>
    );
  }

  return (
    <FilterValueContainer>
      {status === EditableValueStatusEnum.Tag ? (
        <TagAutoComplete
          options={tags}
          onChange={(_, _value) => tagChangeHandler(_value)}
          onBlur={() => onDelete(id)}
        />
      ) : (
        <RestFilterValuePart>{tagKey}</RestFilterValuePart>
      )}
      {expressionPart}
      {status === EditableValueStatusEnum.Value ? (
        <FilterValuePart>
          {Array.isArray(value) && value.length ? (
            <MultipleValues>
              {tagOptions
                .filter(({ value: _value }) => value.includes(_value))
                .map(({ label, value: _value }) => (
                  <MultipleValueItem key={_value}>{label}</MultipleValueItem>
                ))}
            </MultipleValues>
          ) : null}
          <ValueAutoComplete
            multiple={expression?.type === ExpressionEnum.IsOneOf}
            options={tagOptions}
            value={resultValue}
            onChange={(_, _value) => {
              valueChangeHandler(_value);
            }}
            onBlur={valueBlurHandler}
          />
        </FilterValuePart>
      ) : null}
    </FilterValueContainer>
  );
};
