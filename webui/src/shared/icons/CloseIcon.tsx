import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const CloseIcon: FC<IconProps> = (props) => (
  <Icon
    width={24}
    height={24}
    viewBox="0 0 24 24"
    fill="currentColor"
    {...props}
  >
    <path
      d="M18 6L6 18"
      stroke="#050505"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M6 6L18 18"
      stroke="#050505"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
