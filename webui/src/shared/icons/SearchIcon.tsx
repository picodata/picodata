import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const SearchIcon: FC<IconProps> = (props) => (
  <Icon
    width={24}
    height={24}
    viewBox="0 0 24 24"
    fill="transparent"
    {...props}
  >
    <path
      d="M10 17C13.866 17 17 13.866 17 10C17 6.13401 13.866 3 10 3C6.13401 3 3 6.13401 3 10C3 13.866 6.13401 17 10 17Z"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M21 21L15 15"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
