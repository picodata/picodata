import React from "react";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Button, ButtonProps } from "../Button/Button";

import styles from "./Tag.module.scss";

type TagProps = ButtonProps & {
  closeIcon?: boolean;
  onClose?: (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
};

export const Tag: React.FC<TagProps> = (props) => {
  const { closeIcon, rightIcon: RightIcon, onClose, ...other } = props;

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
    <Button {...other} rightIcon={renderIcon()} className={styles.button} />
  );
};
