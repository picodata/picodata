import { Icon, IconProps } from "../icon/Icon";
import { FC } from "react";

export const ChevronUp: FC<IconProps> = (props) => (
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
      d="M26.7071 22.7071C26.3166 23.0976 25.6834 23.0976 25.2929 22.7071L16 13.4142L6.70711 22.7071C6.31658 23.0976 5.68342 23.0976 5.29289 22.7071C4.90237 22.3166 4.90237 21.6834 5.29289 21.2929L15.2929 11.2929C15.6834 10.9024 16.3166 10.9024 16.7071 11.2929L26.7071 21.2929C27.0976 21.6834 27.0976 22.3166 26.7071 22.7071Z"
      fill={props.fill ?? "#1F1F1F"}
    />
  </Icon>
);
