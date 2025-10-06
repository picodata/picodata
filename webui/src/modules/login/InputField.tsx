import { Dispatch, SetStateAction, useState, useCallback } from "react";

import { EyeClosed } from "shared/icons/EyeClosed";
import { EyeOpen } from "shared/icons/EyeOpen";
import { Input, InputProps } from "shared/ui/Input/Input";

import styles from "./LoginPage.module.scss";

export function InputField(
  props: {
    title: string;
    placeholder: string;
    error: string;
    value: string;
    setValue: Dispatch<SetStateAction<string>>;
    toggleable?: boolean;
  } & Omit<InputProps, "onChange">
) {
  const {
    title,
    placeholder,
    error,
    value,
    setValue,
    toggleable,
    ...subprops
  } = props;

  const [textVisible, setTextVisible] = useState(!toggleable);
  const togglePasswordVisibility = useCallback(
    () => setTextVisible((prev) => !prev),
    []
  );

  return (
    <div className={styles.formField}>
      <span className={styles.formFieldTitle}>{title}</span>
      <Input
        {...subprops}
        type={textVisible ? subprops.type ?? "text" : "password"}
        value={value}
        onChange={setValue}
        placeholder={placeholder}
        rightIcon={
          toggleable && (
            <div
              onClick={togglePasswordVisibility}
              className={styles.passwordToggle}
            >
              {textVisible ? <EyeOpen /> : <EyeClosed />}
            </div>
          )
        }
      />
      <span
        className={styles.formFieldError}
        style={{ height: error ? undefined : 0 }}
      >
        {error}
      </span>
    </div>
  );
}
