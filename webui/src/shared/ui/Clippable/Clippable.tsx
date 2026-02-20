import { PropsWithChildren, useRef, useState } from "react";
import { SxProps } from "@mui/material";

import { HiddenWrapper } from "../HiddenWrapper/HiddenWrapper";

import { ClippableIcon } from "./ClippableIcon";
import { Root } from "./StyledComponents";

export interface ClippableProps {
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

  sx?: SxProps;
}

/**
 * Copies text to clipboard when clicking on the icon
 */
export const Clippable = (props: PropsWithChildren<ClippableProps>) => {
  const { inline, text, sx = {}, iconTimeout = 400 } = props;

  const ref = useRef<HTMLDivElement>(null);

  const [isClipping, setClipped] = useState(false);

  return (
    <Root $inline={Boolean(inline)} sx={sx} ref={ref}>
      <ClippableIcon isClipping={isClipping} onClick={copyText} />
      <HiddenWrapper>{props.children}</HiddenWrapper>
    </Root>
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
