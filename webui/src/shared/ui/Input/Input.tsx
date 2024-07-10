import React, { useRef, useState } from "react";
import cn from "classnames";

import styles from "./Input.module.scss";

export type ButtonProps = {
  classes?: {
    container?: string;
  };
  rightIcon?: React.ReactNode;
  disabled?: boolean;
  onFocus?: () => void;
  onBlur?: () => void;
  onChange: (v: string) => void;
  value: string;
} & Omit<React.ButtonHTMLAttributes<HTMLInputElement>, "onChange">;

export const Input: React.FC<ButtonProps> = (props) => {
  const {
    disabled = false,
    rightIcon,
    onBlur,
    onFocus,
    onChange,
    value,
    classes,
    ...inputProps
  } = props;
  const ref = useRef<HTMLInputElement>(null);
  const [isFocused, setIsFocused] = useState(false);

  const handleFocus = (event: React.FocusEvent<HTMLInputElement>) => {
    setIsFocused(true);

    if (onFocus) {
      onFocus(event);
    }
  };

  const handleBlur = (event: React.FocusEvent<HTMLInputElement>) => {
    setIsFocused(false);

    if (onBlur) {
      onBlur(event);
    }
  };

  return (
    <div
      className={cn(
        styles.container,
        isFocused && styles.activeContainer,
        classes?.container
      )}
    >
      <input
        ref={ref}
        {...inputProps}
        value={value}
        className={cn(styles.input, !!rightIcon && styles.inputWithIcon)}
        onFocus={handleFocus}
        onBlur={handleBlur}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
      />
      <div className={styles.content}>
        {rightIcon && <div className={styles.rightIcon}>{rightIcon}</div>}
      </div>
    </div>
  );
};
