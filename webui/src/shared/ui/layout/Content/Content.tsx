import React from "react";
import { SxProps } from "@mui/system/styleFunctionSx";

import { Root } from "./StyledComponents";

type ContentProps = React.HTMLAttributes<HTMLDivElement> & {
  sx?: SxProps;
};

export const Content: React.FC<ContentProps> = (props) => {
  const { ...other } = props;

  return <Root {...other}></Root>;
};
