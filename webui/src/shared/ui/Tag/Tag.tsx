import React from "react";
import cn from "classnames";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Button, ButtonProps } from "../Button/Button";

import styles from "./Tag.module.scss";

type TagProps = ButtonProps & {
  isSelectValue?: boolean;
  closeIcon?: boolean;
  onClose?: (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
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
      {...other}
      rightIcon={renderIcon()}
      className={cn(styles.button, isSelectValue && styles.selectTag)}
    />
  );
};
