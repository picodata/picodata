import { Autocomplete, AutocompleteProps, TextField } from "@mui/material";
import { useEffect, useRef } from "react";

import { TagOption } from "../../model";

type TagOptionAutocompleteProps = AutocompleteProps<
  TagOption,
  boolean,
  false,
  false
>;
type ValueAutoCompleteProps = Partial<TagOptionAutocompleteProps> &
  Pick<TagOptionAutocompleteProps, "options">;
export const ValueAutoComplete = (props: ValueAutoCompleteProps) => {
  const ref = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    ref.current?.focus();
  }, [ref]);

  return (
    <Autocomplete
      sx={{
        width: 200,
      }}
      disableCloseOnSelect={props.multiple}
      getOptionLabel={(option) => option.label}
      renderValue={() => null}
      renderInput={(params) => (
        <TextField variant={"standard"} {...params} inputRef={ref} />
      )}
      openOnFocus={true}
      {...props}
    />
  );
};
