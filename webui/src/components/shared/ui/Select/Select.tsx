import { useMemo } from "react";
import RNSelect, { Props as RNSelectProps } from "react-select";
import cn from "classnames";

import { Option } from "../Option/Option";

import styles from "./Select.module.scss";

import type { TOption } from "./types";

export type { TOption };

type SelectProps<T> = RNSelectProps<T> & {
  size?: "normal";
};

export const Select = <T,>(props: SelectProps<T>) => {
  const { size = "normal", classNames: classNamesProps = {}, ...other } = props;

  const classNames = useMemo<SelectProps<T>["classNames"]>(() => {
    return {
      ...classNamesProps,
      control: (state) => {
        let controlClassName = "";
        if (state.isDisabled) controlClassName = styles.disabledControl;
        if (state.isFocused) controlClassName = styles.focusedControl;

        return cn(
          styles.commonControl,
          controlClassName,
          styles[size],
          classNamesProps?.control?.(state)
        );
      },
    };
  }, [size, classNamesProps]);

  return (
    <RNSelect
      {...other}
      classNames={classNames}
      components={{
        Option: (args) => (
          <Option isSelected={args.isSelected} {...args.innerProps}>
            {args.children}
          </Option>
        ),
        IndicatorSeparator: null,
      }}
    />
  );
};
