import { useRef, useState } from "react";
import { SxProps } from "@mui/system/styleFunctionSx";

import { Root, StyledInput, IconContent, RightIcon } from "./StyledComponents";

export type InputProps = {
  classes?: {
    container?: string;
  };
  rightIcon?: React.ReactNode;
  disabled?: boolean;
  onFocus?: () => void;
  onBlur?: () => void;
  onChange: (v: string) => void;
  value: string;
  containerSx?: SxProps;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, "onChange">;

export const Input: React.FC<InputProps> = (props) => {
  const {
    disabled = false,
    containerSx = {},
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
    <Root $isFocused={isFocused} sx={containerSx}>
      <StyledInput
        ref={ref}
        {...inputProps}
        value={value}
        onFocus={handleFocus}
        onBlur={handleBlur}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
      />
      <IconContent>
        {rightIcon && <RightIcon>{rightIcon}</RightIcon>}
      </IconContent>
    </Root>
  );
};
