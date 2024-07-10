import React, { useId } from "react";
import { Tooltip } from "react-tooltip";

import { SuggestedIcon } from "shared/icons/SuggestedIcon";

import styles from "./Prompt.module.scss";

type PromptIconProps = {
  children: React.ReactNode;
};

export const PromptIcon: React.FC<PromptIconProps> = (props) => {
  const { children } = props;

  const id = useId();

  return (
    <>
      <div data-tooltip-id={id} className={styles.iconContainer}>
        <SuggestedIcon height={16} width={16} />
      </div>
      <Tooltip
        opacity={1}
        clickable
        id={id}
        place="right"
        className={styles.tooltip}
        classNameArrow={styles.arrow}
      >
        <div className={styles.content}>{children}</div>
      </Tooltip>
    </>
  );
};
