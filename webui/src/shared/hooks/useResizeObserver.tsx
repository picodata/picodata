import { MutableRefObject, useEffect } from "react";

export const useResizeObserver = ({
  handleSize,
  ref,
}: {
  handleSize: () => void;
  ref: MutableRefObject<HTMLElement | null | undefined>;
}) => {
  useEffect(() => {
    handleSize();

    const link = ref.current ? new ResizeObserver(handleSize) : undefined;

    if (link && ref.current) {
      link.observe(ref.current);
    }

    return () => {
      if (link) {
        link.disconnect();
      }
    };
  }, [handleSize, ref]);
};
