import { useCallback, useEffect, useState } from "react";

export const useHiddenRefs = (refs: React.RefObject<HTMLDivElement>[]) => {
  const [isHidden, setIsHidden] = useState<boolean>();

  const handleSize = useCallback(() => {
    const newIsHidden = !!refs.find((ref) => {
      return ref.current
        ? ref.current.scrollWidth > ref.current.clientWidth ||
            ref.current.scrollHeight > ref.current.clientHeight
        : undefined;
    });

    setIsHidden(newIsHidden);
  }, [refs]);

  useEffect(() => {
    handleSize();

    const links: ResizeObserver[] = [];
    refs.forEach((ref) => {
      const link = ref.current ? new ResizeObserver(handleSize) : undefined;

      if (link && ref.current) {
        link.observe(ref.current);
        links.push(link);
      }
    });

    return () => {
      links.forEach((l) => l.disconnect());
    };
  }, [handleSize, refs]);

  return isHidden;
};
