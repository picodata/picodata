import { PropsWithChildren, useRef, useState } from "react";
import cn from "classnames";

import { HiddenWrapper } from "../HiddenWrapper/HiddenWrapper";

import { ClippableIcon } from "./ClippableIcon";

import styles from "./Clippable.module.scss";

export interface ClippableProps {
  className?: string;

  /**
   * For how long the "copied" icon should be displayed (ms).
   *
   * Default is 400ms
   */
  iconTimeout?: number;

  /**
   * Set specific text to copy on-click,
   * or pre-process text from children
   */
  text?: string | ((original: string) => string);

  /**
   * Shorthand to display icon in line with the other contents
   */
  inline?: boolean;
}

/**
 * Copies text to clipboard when clicking on the icon
 */
export const Clippable = (props: PropsWithChildren<ClippableProps>) => {
  const { className, inline, text, iconTimeout = 400 } = props;

  const ref = useRef<HTMLDivElement>(null);

  const [isClipping, setClipped] = useState(false);

  return (
    <div
      ref={ref}
      className={inline ? cn(styles.inline, className) : className}
    >
      <ClippableIcon isClipping={isClipping} onClick={copyText} />
      <HiddenWrapper>{props.children}</HiddenWrapper>
    </div>
  );

  function copyText() {
    const clipText =
      typeof text === "function"
        ? text(ref.current?.innerText ?? "")
        : text || ref.current?.innerText;

    if (!clipText) {
      return;
    }

    navigator.clipboard.writeText(clipText);

    setClipped(true);
    setTimeout(() => setClipped(false), iconTimeout);
  }
};
