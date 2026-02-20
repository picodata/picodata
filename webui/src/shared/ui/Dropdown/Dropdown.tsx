import { MouseEvent } from "react";

import { Option } from "../Option/Option";

import { Root } from "./StyledComponents";

type DropdownItem<T extends string | number> = {
  value: T;
  label: string;
};

export type DropdownProps<T extends string | number> = {
  items: DropdownItem<T>[];
  upDirection?: boolean;
  value?: T;
  onClick?: (e: MouseEvent<HTMLDivElement>) => void;
  onChange?: (value: T) => void;
};

export const Dropdown = <T extends string | number>(
  props: DropdownProps<T>
) => {
  return (
    <Root $upDirection={Boolean(props.upDirection)} onClick={props.onClick}>
      {props.items.map((item) => {
        const isSelected = props.value === item.value;

        return (
          <Option
            key={item.value}
            isSelected={isSelected}
            onMouseDown={() => props.onChange?.(item.value)}
          >
            {item.label}
          </Option>
        );
      })}
    </Root>
  );
};
