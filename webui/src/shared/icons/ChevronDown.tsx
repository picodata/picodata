import { FC } from "react";

import { Icon, IconProps } from "./Icon";

export const ChevronDown: FC<IconProps> = (props) => (
  <Icon
    width={32}
    height={32}
    viewBox="0 0 32 32"
    fill="currentColor"
    {...props}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M3.96967 8.46967C4.26256 8.17678 4.73744 8.17678 5.03033 8.46967L12 15.4393L18.9697 8.46967C19.2626 8.17678 19.7374 8.17678 20.0303 8.46967C20.3232 8.76256 20.3232 9.23744 20.0303 9.53033L12.5303 17.0303C12.2374 17.3232 11.7626 17.3232 11.4697 17.0303L3.96967 9.53033C3.67678 9.23744 3.67678 8.76256 3.96967 8.46967Z"
      fill={props.fill ?? "#1F1F1F"}
    />
  </Icon>
);
