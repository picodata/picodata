import React from "react";

import { useMount } from "./useMount";

export const useOutsideClickEvent = (
  ref: React.RefObject<Element>,
  outsideCallback: () => void
) => {
  useMount(() => {
    const handler = (event: MouseEvent) => {
      if (!ref.current) return;

      if (
        event.target instanceof HTMLElement &&
        !ref.current.contains(event.target)
      ) {
        outsideCallback();
      }
    };

    document.addEventListener("click", handler, true);

    return () => {
      document.removeEventListener("click", handler);
    };
  });
};
