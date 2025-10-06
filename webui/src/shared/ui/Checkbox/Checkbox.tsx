import React, { useMemo, useRef } from "react";
import cn from "classnames";

import { CheckIcon } from "shared/icons/CheckIcon";

import styles from "./Checkbox.module.scss";

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
    <div className={cn(styles.container, subprops.className)}>
      <input
        className={styles.input}
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
      <CheckIcon className={styles.check} />
      <label htmlFor={id} className={styles.content}>
        {children}
      </label>
    </div>
  );
};
