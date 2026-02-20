import React from "react";
import { SxProps } from "@mui/material";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Button, ButtonProps } from "../Button/Button";

import { sx } from "./StyledComponents";

type TagProps = ButtonProps & {
  isSelectValue?: boolean;
  closeIcon?: boolean;
  onClose?: (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
  sx?: SxProps;
};

export const Tag: React.FC<TagProps> = (props) => {
  const {
    closeIcon,
    rightIcon: RightIcon,
    onClose,
    isSelectValue,
    ...other
  } = props;

  const renderIcon = () => {
    if (RightIcon) return RightIcon;

    if (closeIcon)
      return (
        <div
          onClick={(event) => {
            event.stopPropagation();
            onClose?.(event);
          }}
        >
          <CircleCloseIcon />
        </div>
      );
  };

  return (
    <Button
      sx={isSelectValue ? sx : undefined}
      {...other}
      rightIcon={renderIcon()}
    />
  );
};
