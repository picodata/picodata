import { Autocomplete, Box, styled, TextField } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import RttIcon from "@mui/icons-material/Rtt";

import {
  FilterProps,
  Tag,
  EditableFilterValue,
  EditableValueStatusEnum,
  ExpressionEnum,
} from "../model";
import {
  getEditableFilterValue,
  getFilterValueByEditableFilterValue,
  SEARCH_TEXT_KEY,
} from "../lib";
import { useTranslation } from "../../../intl";

import { TagOption } from "./TagOption";
import { FilterValues } from "./FilterValues";

const useSearchTextTag = () => {
  const { translation } = useTranslation();
  return useMemo(
    () => ({
      id: crypto.randomUUID(),
      key: SEARCH_TEXT_KEY,
      label: translation.components.filterTags.searchForThisText,
      icon: RttIcon,
      options: [],
    }),
    [translation]
  );
};

const Root = styled(Box)<{ withGap: boolean }>(({ withGap }) => ({
  display: "grid",
  gridTemplateColumns: "minmax(0px, min-content) minmax(200px, 1fr)",
  overflow: "hidden",
  gap: withGap ? 10 : 0,
}));

export const Filter = (props: FilterProps) => {
  const { tags = [], value, onChange } = props;
  const [valueState, setValueState] = useState<EditableFilterValue[]>(
    getEditableFilterValue(value)
  );
  const [inputValue, setInputValue] = useState("");
  const searchTag = useSearchTextTag();
  const resultTags = useMemo(() => [...tags, searchTag], [tags, searchTag]);

  useEffect(() => {
    setValueState(getEditableFilterValue(value));
  }, [value]);

  const changeHandler = (tag: Tag | null) => {
    if (tag?.key === SEARCH_TEXT_KEY) {
      onChange(
        getFilterValueByEditableFilterValue([
          ...valueState,
          {
            id: crypto.randomUUID(),
            tagKey: tag?.key,
            status: EditableValueStatusEnum.Done,
            expression: {
              type: ExpressionEnum.Is,
              label: "is",
              description: "==",
            },
            value: inputValue,
          },
        ])
      );
    }
    if (tag) {
      setValueState((_valueState) => [
        ..._valueState,
        {
          id: crypto.randomUUID(),
          tagKey: tag?.key,
          status: EditableValueStatusEnum.Expression,
          expression: undefined,
          value: undefined,
        },
      ]);
    }
  };

  const valueDeleteHandler = (valueId: string) => {
    const resultValueState = valueState.filter(({ id }) => id !== valueId);
    if (resultValueState.length !== value.length) {
      onChange(getFilterValueByEditableFilterValue(resultValueState));
    } else {
      setValueState(resultValueState);
    }
  };

  const filterValueChangeHandler = (
    editableFilterValue: EditableFilterValue
  ) => {
    setValueState((_valueState) => {
      return _valueState.map((_filterValue) =>
        _filterValue.id === editableFilterValue.id
          ? editableFilterValue
          : _filterValue
      );
    });
    if (editableFilterValue.status === "Done") {
      onChange(
        getFilterValueByEditableFilterValue(
          valueState.map((item) =>
            item.id === editableFilterValue.id ? editableFilterValue : item
          )
        )
      );
    }
  };

  return (
    <Root withGap={!!valueState.length}>
      <Box overflow={"auto"}>
        <FilterValues
          value={valueState}
          onDelete={valueDeleteHandler}
          onChange={filterValueChangeHandler}
          tags={resultTags}
        />
      </Box>
      <Box>
        <Autocomplete
          getOptionDisabled={(option) =>
            option.key === SEARCH_TEXT_KEY && !inputValue
          }
          inputValue={inputValue}
          onInputChange={(_, _value) => {
            setInputValue(_value);
          }}
          options={resultTags}
          getOptionLabel={(option) => option.label}
          renderOption={TagOption}
          value={null}
          renderValue={() => null}
          filterOptions={(options, { inputValue: _inputValue }) => {
            return options.filter((option) => {
              return (
                option.key === SEARCH_TEXT_KEY ||
                option.label.includes(_inputValue)
              );
            });
          }}
          onChange={(_, _value) => {
            changeHandler(_value);
          }}
          renderInput={(params) => <TextField {...params} />}
          autoHighlight={true}
        />
      </Box>
    </Root>
  );
};
