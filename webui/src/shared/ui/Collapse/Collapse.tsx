import React, {
  useRef,
  useState,
  useEffect,
  useCallback,
  PropsWithChildren,
} from "react";
import { Transition, TransitionStatus } from "react-transition-group";

import { useResizeObserver } from "shared/hooks/useResizeObserver";

import { Content } from "./StyledComponent";

type CollapseProps = {
  isOpen: boolean;
  style?: React.CSSProperties;
  timingOptions?: {
    exit?: number;
    appear?: number;
    enter?: number;
  };
};

export const Collapse: React.FC<PropsWithChildren<CollapseProps>> = ({
  children,
  isOpen,
  timingOptions = {},
  style,
}) => {
  const [height, setHeight] = useState<number>(0);
  const contentRef = useRef<HTMLDivElement>(null);

  const handleSize = useCallback(() => {
    setHeight(contentRef.current?.scrollHeight || 0);
  }, []);

  useEffect(() => {
    handleSize();
  }, [handleSize, isOpen]);

  useResizeObserver({ handleSize, ref: contentRef });

  const transitionStyles: Record<TransitionStatus, React.CSSProperties> = {
    entering: { height },
    entered: { height: "auto" },
    exiting: { height },
    exited: { height: 0 },
    unmounted: {},
  };

  const { exit = 0, appear = 300, enter = 300 } = timingOptions;

  return (
    <Transition in={isOpen} timeout={{ exit, appear, enter }}>
      {(state) => (
        <Content
          ref={contentRef}
          style={{
            ...transitionStyles[state],
            ...style,
          }}
        >
          {children}
        </Content>
      )}
    </Transition>
  );
};
