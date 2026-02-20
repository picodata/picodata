import { FC, ReactNode, MouseEventHandler, CSSProperties } from "react";

export interface IconProps {
  height?: number;
  width?: number;
  fill?: string;
  className?: string;
  stroke?: string;
  viewBox?: string;
  children?: ReactNode;
  onClick?: MouseEventHandler<SVGElement | HTMLElement>;
  dataTest?: string;
  style?: CSSProperties;
}

export const Icon: FC<IconProps> = ({
  width = 24,
  height = 24,
  viewBox = "0 0 24 24",
  fill = "#696B6D",
  style = {},
  stroke,
  children,
  className,
  onClick,
  dataTest,
}) => (
  <svg
    width={width}
    height={height}
    viewBox={viewBox}
    fill={fill}
    stroke={stroke}
    className={className}
    onClick={onClick}
    data-test={dataTest}
    xmlns="http://www.w3.org/2000/svg"
    style={style}
  >
    {children}
  </svg>
);
