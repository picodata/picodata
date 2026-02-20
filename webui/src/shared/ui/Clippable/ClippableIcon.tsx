import React from "react";

import { LinkSystemIcon } from "shared/icons/LinkSystemIcon";
import { LinkBreakSystemIcon } from "shared/icons/LinkBreakSystemIcon";

import { ClipIcon, getIconCopiedSx, getIconCopySx } from "./StyledComponents";

export interface ClippableIconProps {
  isClipping?: boolean;
  onClick?: VoidFunction;
}

export const ClippableIcon: React.FC<ClippableIconProps> = (props) => {
  const { onClick, isClipping } = props;
  return (
    <ClipIcon $isClipping={Boolean(isClipping)} onClick={onClick}>
      <LinkSystemIcon
        width={16}
        height={16}
        style={getIconCopySx(Boolean(isClipping))}
      />
      <LinkBreakSystemIcon
        width={16}
        height={16}
        style={getIconCopiedSx(Boolean(isClipping))}
      />
    </ClipIcon>
  );
};
