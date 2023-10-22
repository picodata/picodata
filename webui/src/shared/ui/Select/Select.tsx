import { useMemo } from "react";
import RNSelect, { Props as RNSelectProps } from "react-select";
import cn from "classnames";

import { Option } from "../Option/Option";
import { Tag } from "../Tag/Tag";

import type { TOption } from "./types";

import styles from "./Select.module.scss";

export type { TOption };

type SelectProps<T> = RNSelectProps<T> & {
  size?: "normal";
};

export const Select = <T,>(props: SelectProps<T>) => {
  const { size = "normal", classNames: classNamesProps = {}, ...other } = props;

  const classNames = useMemo<SelectProps<T>["classNames"]>(() => {
    return {
      ...classNamesProps,
      valueContainer: (state) => {
        let valueContainerClassName = "";

        if (state.isMulti) valueContainerClassName = styles.multiValueContainer;

        return cn(valueContainerClassName);
      },
      control: (state) => {
        let controlClassName = "";
        if (state.isDisabled) controlClassName = styles.disabledControl;
        if (state.isFocused) controlClassName = styles.focusedControl;

        return cn(
          styles.commonControl,
          controlClassName,
          state.isMulti ? styles.multiControl : styles.control,
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
        MultiValue: (args) => (
          <Tag
            size="extraSmall"
            onIconClick={args.removeProps.onClick}
            className={args.innerProps?.className}
          >
            {args.children}
          </Tag>
        ),
      }}
    />
  );
};
