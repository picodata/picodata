import React, { useRef, ReactNode, useId } from "react";
import cn from "classnames";
import { Tooltip } from "react-tooltip";

import { useHiddenRefs } from "shared/hooks/useHiddenRefs";

import styles from "./HiddenWrapper.module.scss";

export type HiddenWrapperProps = {
  children: ReactNode;
  className?: string;
  twoLine?: boolean;
  onMouseEnter?: React.MouseEventHandler<HTMLDivElement>;
  onMouseLeave?: React.MouseEventHandler<HTMLDivElement>;
  style?: React.CSSProperties;
};

export const HiddenWrapper = ({
  children,
  className,
  onMouseEnter,
  onMouseLeave,
  style = {},
  twoLine = false,
}: HiddenWrapperProps) => {
  const id = useId();

  const ref = useRef<HTMLDivElement>(null);

  const isHidden = useHiddenRefs([ref]);

  return (
    <>
      <div
        data-tooltip-id={id}
        ref={ref}
        style={style}
        className={cn(styles.text, className, twoLine && styles.twoLiner)}
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
      >
        {children}
      </div>
      <Tooltip
        opacity={1}
        hidden={!isHidden}
        id={id}
        clickable
        className={styles.tooltip}
        place="bottom"
        noArrow
        offset={0}
      >
        <div className={cn(styles.overlay)}>{children}</div>
      </Tooltip>
    </>
  );
};
