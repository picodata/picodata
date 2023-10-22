import { useMemo } from "react";
import RNSelect, { MultiValue, Props as RNSelectProps } from "react-select";
import cn from "classnames";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Option } from "../Option/Option";
import { Tag } from "../Tag/Tag";

import type { TOption } from "./types";

import styles from "./Select.module.scss";

export type { TOption };

type SelectProps<T extends TOption> = RNSelectProps<T> & {
  size?: "normal";
  showMoreButtonCount?: number;
};

export const Select = <T extends TOption>(props: SelectProps<T>) => {
  const {
    size = "normal",
    classNames: classNamesProps = {},
    showMoreButtonCount = 3,
    ...other
  } = props;

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
      closeMenuOnSelect={other.isMulti ? false : true}
      {...other}
      classNames={classNames}
      components={{
        Option: (args) => (
          <Option isSelected={args.isSelected} {...args.innerProps}>
            {args.children}
          </Option>
        ),
        IndicatorSeparator: null,
        MultiValue: (args) => {
          const value = args.selectProps.value as MultiValue<T>;

          if (!args.selectProps.menuIsOpen) {
            if (args.index > showMoreButtonCount) {
              return null;
            }

            if (args.index === showMoreButtonCount) {
              return (
                <Tag
                  size="extraSmall"
                  theme="secondary"
                  className={args.innerProps?.className}
                >
                  See all ({value.length})
                </Tag>
              );
            }
          }

          return (
            <Tag
              size="extraSmall"
              className={args.innerProps?.className}
              rightIcon={
                <div {...args.removeProps}>
                  <CircleCloseIcon />
                </div>
              }
            >
              {args.children}
            </Tag>
          );
        },
      }}
    />
  );
};
