import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const CheckIcon: FC<IconProps> = (props) => (
  <Icon
    width={16}
    height={16}
    viewBox="0 0 16 16"
    stroke="currentColor"
    fill="none"
    {...props}
  >
    <path
      d="M13.3334 4L6.00008 11.3333L2.66675 8"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
