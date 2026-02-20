import React, { useRef, ReactNode, useId } from "react";
import { PlacesType, Tooltip } from "react-tooltip";
import { SxProps } from "@mui/material";

import { useHiddenRefs } from "shared/hooks/useHiddenRefs";

import styles from "./HiddenWrapper.module.css";
import { Overlay, Root } from "./StyledComponents";

export type HiddenWrapperProps = {
  children: ReactNode;
  sx?: SxProps;
  twoLine?: boolean;
  place?: PlacesType;
  onMouseEnter?: React.MouseEventHandler<HTMLDivElement>;
  onMouseLeave?: React.MouseEventHandler<HTMLDivElement>;
  style?: React.CSSProperties;
};

export const HiddenWrapper = ({
  children,
  sx,
  place,
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
      <Root
        data-tooltip-id={id}
        ref={ref}
        style={style}
        sx={sx}
        $twoLine={twoLine}
        // className={cn(styles.text, className, twoLine && styles.twoLiner)} //ToDo
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
      >
        {children}
      </Root>
      <Tooltip
        opacity={1}
        hidden={!isHidden}
        id={id}
        clickable
        className={styles.tooltip}
        place={place ?? "bottom"}
        noArrow
        offset={0}
      >
        <Overlay>{children}</Overlay>
      </Tooltip>
    </>
  );
};
