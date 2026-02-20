import React from "react";

import { Root } from "./StyledComponents";

type PageContainerProps = React.HTMLAttributes<HTMLDivElement>;

export const PageContainer: React.FC<PageContainerProps> = (props) => {
  const { className, ...other } = props;

  return <Root {...other}></Root>;
};
