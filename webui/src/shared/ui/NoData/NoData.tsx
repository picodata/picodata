import { PropsWithChildren } from "react";

import { Root } from "./StyledComponents";

export const NoData = ({ children }: PropsWithChildren) => {
  return <Root>{children}</Root>;
};
