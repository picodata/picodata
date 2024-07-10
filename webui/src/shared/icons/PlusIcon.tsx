import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const PlusIcon: FC<IconProps> = (props) => (
  <Icon
    width={16}
    height={16}
    viewBox="0 0 16 16"
    fill="currentColor"
    {...props}
  >
    <path
      d="M8 3.33301V12.6663"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M3.3335 8H12.6668"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
