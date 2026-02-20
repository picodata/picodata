import React, { useMemo, useRef } from "react";

import { CheckIcon } from "shared/icons/CheckIcon";

import { checkSx, Content, Root, StyleInput } from "./StyledComponents";

export type CheckboxProps = {
  children?: React.ReactNode;
  disabled?: boolean;
} & React.InputHTMLAttributes<HTMLInputElement>;

let unique = 0;

export const Checkbox: React.FC<CheckboxProps> = (props) => {
  const { children, disabled = false, ...subprops } = props;
  const ref = useRef<HTMLInputElement>(null);

  const id = useMemo(
    () => subprops.id ?? `checkbox-${unique++}`,
    [subprops.id]
  );

  return (
    <Root>
      <StyleInput
        type="checkbox"
        {...subprops}
        id={id}
        disabled={disabled}
        ref={ref}
        onChange={(e) => {
          if (subprops.onChange) {
            subprops.onChange(e);
          }
          ref.current?.focus();
        }}
        checked={!!subprops.checked || !!subprops.value}
      />
      <CheckIcon style={checkSx} />
      <Content htmlFor={id}>{children}</Content>
    </Root>
  );
};
