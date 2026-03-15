import { Box, styled } from "@mui/material";

import { EditableFilterValue, EditableValueStatusEnum, Tag } from "../model";

import { ValueElement } from "./FilterValue";
import { FilterValueForm } from "./FilterValueForm";

const Root = styled(Box)({
  height: 40,
  display: "flex",
  gap: 6,
});

type FilterValuesProps = {
  value: EditableFilterValue[];
  onDelete: (valueId: string) => void;
  onChange: (editableFilterValue: EditableFilterValue) => void;
  tags: Tag[];
};

export const FilterValues = ({
  value: values,
  onDelete,
  onChange,
  tags,
}: FilterValuesProps) => {
  return (
    <Root>
      {values.map((value) =>
        value.status === EditableValueStatusEnum.Done ? (
          <ValueElement
            tags={tags}
            value={value}
            key={value.id}
            onDelete={onDelete}
            onChange={onChange}
          />
        ) : (
          <FilterValueForm
            tags={tags}
            value={value}
            key={value.id}
            onChange={onChange}
            onDelete={onDelete}
          />
        )
      )}
    </Root>
  );
};
