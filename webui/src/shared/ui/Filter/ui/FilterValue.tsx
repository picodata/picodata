import { Box, styled } from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";

import { EditableFilterValue, EditableValueStatusEnum, Tag } from "../model";
import { SEARCH_TEXT_KEY } from "../lib";

import { FilterValueContainer, RestFilterValuePart } from "./common";

const Actions = styled(Box)({
  display: "flex",
  flexDirection: "column",
});

const StyledCloseIcon = styled(CloseIcon)(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  width: 14,
  "&:hover": {
    fill: theme.palette.primary.main,
  },
  marginLeft: 6,
  marginRight: 6,
}));

type FilterValueProps = {
  tags: Tag[];
  onDelete: (valueId: string) => void;
  value: EditableFilterValue;
  onChange: (editableFilterValue: EditableFilterValue) => void;
};
export const ValueElement = ({
  tags,
  value,
  onDelete,
  onChange,
}: FilterValueProps) => {
  const { id, tagKey, expression, value: itemValue } = value;

  const tagClickHandler = () => {
    onChange({
      ...value,
      tagKey: undefined,
      status: EditableValueStatusEnum.Tag,
      expression: undefined,
      value: undefined,
    });
  };
  const expressionClickHandler = () => {
    onChange({
      ...value,
      status: EditableValueStatusEnum.Expression,
      expression: undefined,
      value: undefined,
    });
  };
  const valueClickHandler = () => {
    onChange({
      ...value,
      status: EditableValueStatusEnum.Value,
      value: Array.isArray(value.value) ? value.value : undefined,
    });
  };
  const deleteClickHandler = () => {
    onDelete(id);
  };
  const searchTextClickHandler = () => {
    onChange({
      ...value,
      status: EditableValueStatusEnum.Value,
    });
  };

  const currentTagLabel = tags.find(({ key }) => key === tagKey)?.label || "";
  return (
    <FilterValueContainer>
      {tagKey === SEARCH_TEXT_KEY ? (
        <RestFilterValuePart
          onClick={searchTextClickHandler}
          sx={{ paddingLeft: "10px", paddingRight: "10px" }}
        >
          {itemValue}
        </RestFilterValuePart>
      ) : (
        <>
          <RestFilterValuePart
            onClick={tagClickHandler}
            sx={{ paddingLeft: "10px" }}
          >
            {currentTagLabel}
          </RestFilterValuePart>
          <RestFilterValuePart onClick={expressionClickHandler}>
            {expression?.description || ""}
          </RestFilterValuePart>
          <RestFilterValuePart
            onClick={valueClickHandler}
            sx={{ paddingRight: "10px" }}
          >
            {Array.isArray(itemValue) ? itemValue.join(" | ") : itemValue}
          </RestFilterValuePart>
        </>
      )}
      <Actions>
        <StyledCloseIcon onClick={deleteClickHandler} />
      </Actions>
    </FilterValueContainer>
  );
};
