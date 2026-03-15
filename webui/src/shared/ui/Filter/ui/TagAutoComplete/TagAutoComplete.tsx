import { Autocomplete, AutocompleteProps, TextField } from "@mui/material";
import { useEffect, useRef } from "react";

import { Tag } from "../../model";

type TagAutocompleteProps = AutocompleteProps<Tag, false, false, false>;
type TagAutoCompleteProps = Partial<TagAutocompleteProps> &
  Pick<TagAutocompleteProps, "options">;
export const TagAutoComplete = (props: TagAutoCompleteProps) => {
  const ref = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    ref.current?.focus();
  }, [ref]);

  return (
    <Autocomplete
      sx={{
        width: 200,
      }}
      getOptionLabel={(option) => option.label}
      value={null}
      renderValue={() => null}
      renderInput={(params) => (
        <TextField variant={"standard"} {...params} inputRef={ref} />
      )}
      openOnFocus={true}
      {...props}
    />
  );
};
