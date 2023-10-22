import React from "react";

import { CircleCloseIcon } from "shared/icons/CircleCloseIcon";

import { Button, ButtonProps } from "../Button/Button";

import styles from "./Tag.module.scss";

type TagProps = ButtonProps & {
  onIconClick?: (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
};

export const Tag: React.FC<TagProps> = (props) => {
  const { onIconClick, ...other } = props;

  return (
    <Button
      {...other}
      rightIcon={
        <div
          onClick={
            onIconClick
              ? (event) => {
                  event.stopPropagation();
                  onIconClick?.(event);
                }
              : undefined
          }
        >
          <CircleCloseIcon />
        </div>
      }
      className={styles.button}
    />
  );
};
