import { TextField, TextFieldProps } from "@mui/material";
import { useEffect, useRef, useState } from "react";

import { EditableFilterValue } from "../../model";

type SearchTextFieldProps = TextFieldProps & {
  filterValue: EditableFilterValue;
  onBlurChange: (value: string) => void;
};
export const SearchTextField = ({
  filterValue,
  onBlurChange,
  ...props
}: SearchTextFieldProps) => {
  const ref = useRef<HTMLInputElement | null>(null);
  const [value, setValue] = useState((filterValue.value || "") as string);

  useEffect(() => {
    ref.current?.focus();
  }, [ref]);

  const blurHandler = () => {
    onBlurChange(value);
  };

  return (
    <TextField
      {...props}
      onBlur={blurHandler}
      value={value}
      onKeyDown={(event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          onBlurChange(value);
        }
      }}
      onChange={(event) => setValue(event.target.value)}
      inputRef={ref}
      sx={{
        minWidth: 200,
      }}
    />
  );
};
