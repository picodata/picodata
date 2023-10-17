import { MouseEvent } from "react";
import classNames from "classnames";

import styles from "./Dropdown.module.scss";
import { Option } from "../Option/Option";

type DropdownItem<T extends string | number> = {
  value: T;
  label: string;
};

export type DropdownProps<T extends string | number> = {
  items: DropdownItem<T>[];
  className?: string;
  upDirection?: boolean;
  value?: T;
  onClick?: (e: MouseEvent<HTMLDivElement>) => void;
  onChange?: (value: T) => void;
};

export const Dropdown = <T extends string | number>(
  props: DropdownProps<T>
) => {
  return (
    <div
      className={classNames(
        styles.container,
        props.upDirection && styles.upDirection,
        props.className
      )}
      onClick={props.onClick}
    >
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
    </div>
  );
};
