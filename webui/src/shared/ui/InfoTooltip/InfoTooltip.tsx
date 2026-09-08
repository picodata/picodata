import { ReactNode } from "react";
import { Tooltip } from "@mui/material";

import { StyledInfoIcon } from "./StyledComponents";

type InfoTooltipProps = {
  title: ReactNode;
};

export const InfoTooltip = ({ title }: InfoTooltipProps) => (
  <Tooltip title={title} arrow placement="top">
    <StyledInfoIcon />
  </Tooltip>
);
