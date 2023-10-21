import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const LeaderIcon: FC<IconProps> = (props) => (
  <Icon
    width={16}
    height={16}
    viewBox="0 0 16 16"
    fill="currentColor"
    {...props}
  >
    <path
      d="M8.50004 1.33301L10.56 5.50634L15.1667 6.17968L11.8334 9.42634L12.62 14.013L8.50004 11.8463L4.38004 14.013L5.16671 9.42634L1.83337 6.17968L6.44004 5.50634L8.50004 1.33301Z"
      fill="#FFBA79"
      stroke="#F7941D"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
