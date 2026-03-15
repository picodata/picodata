import { Autocomplete, AutocompleteProps, TextField } from "@mui/material";
import { useEffect, useRef } from "react";

import { expressionOptions } from "../../lib";
import { Expression } from "../../model";

import { ExpressionOption } from "./Option";

type ExpressionAutoCompleteProps = Partial<
  AutocompleteProps<Expression, false, false, false>
>;
export const ExpressionAutoComplete = (props: ExpressionAutoCompleteProps) => {
  const ref = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    ref.current?.focus();
  }, [ref]);
  return (
    <Autocomplete
      sx={{
        width: 200,
      }}
      options={expressionOptions}
      getOptionLabel={(option) => option.label}
      value={null}
      renderOption={ExpressionOption}
      renderValue={() => null}
      renderInput={(params) => (
        <TextField variant={"standard"} {...params} inputRef={ref} />
      )}
      openOnFocus={true}
      {...props}
    />
  );
};
