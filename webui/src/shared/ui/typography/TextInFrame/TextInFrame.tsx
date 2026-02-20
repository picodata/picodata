import { PropsWithChildren } from "react";

import { Root } from "./StyledComponents";

export const TextInFrame = (props: PropsWithChildren) => {
  return <Root>{props.children}</Root>;
};
