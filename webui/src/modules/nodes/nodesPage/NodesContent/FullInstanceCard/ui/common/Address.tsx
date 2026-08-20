import { PropsWithChildren } from "react";
import { Box, styled } from "@mui/material";

const addressRegExp = /^(.*):([^:]*)$/;

const Root = styled(Box)({
  display: "grid",
  gridTemplateColumns: "1fr min-content",
});
const Hidden = styled(Box)({
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
});
type AddressProps = PropsWithChildren;
export const Address = ({ children }: AddressProps) => {
  if (!children || typeof children !== "string") {
    return null;
  }

  const match = children.match(addressRegExp);

  return match && match[1] && match[2] ? (
    <Root>
      <Hidden>{match[1]}</Hidden>
      <Box>:{match[2]}</Box>
    </Root>
  ) : null;
};
