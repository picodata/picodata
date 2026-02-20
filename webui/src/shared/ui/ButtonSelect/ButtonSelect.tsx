import { useState } from "react";

import { Button, ButtonProps } from "../Button/Button";
import { Dropdown, DropdownProps } from "../Dropdown/Dropdown";

import { Root } from "./StyledComponents";

export type ButtonSelectProps<T extends string | number> = Omit<
  ButtonProps,
  "onChange"
> &
  Pick<DropdownProps<T>, "value" | "items" | "onChange">;

export const ButtonSelect = <T extends string | number>(
  props: ButtonSelectProps<T>
) => {
  const { children, items, value, onChange, ...buttonProps } = props;
  const [isOpen, setIsOpen] = useState(false);

  return (
    <Root>
      <Button
        {...buttonProps}
        onClick={() => setIsOpen(!isOpen)}
        onBlur={() => setIsOpen(false)}
      >
        {children}
      </Button>
      {isOpen && <Dropdown items={items} value={value} onChange={onChange} />}
    </Root>
  );
};
