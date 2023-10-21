import React from "react";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Button, ButtonProps } from "../Button/Button";

import styles from "./Tag.module.scss";

type TagProps = ButtonProps & {
  onIconClick?: () => void;
};

export const Tag: React.FC<TagProps> = (props) => {
  const { onIconClick, ...other } = props;

  return (
    <Button
      {...other}
      rightIcon={
        <CircleCloseIcon
          onClick={
            onIconClick
              ? (event) => {
                  event.stopPropagation();
                  onIconClick?.();
                }
              : undefined
          }
        />
      }
      className={styles.button}
    />
  );
};
