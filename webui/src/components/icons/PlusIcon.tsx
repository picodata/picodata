import { Icon, IconProps } from "../icon/Icon";
import { FC } from "react";

export const PlusIcon: FC<IconProps> = (props) => (
  <Icon
    width={16}
    height={16}
    viewBox="0 0 16 16"
    fill="currentColor"
    {...props}
  >
    <path
      d="M8 3.33398V12.6673"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M3.33398 8H12.6673"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </Icon>
);
