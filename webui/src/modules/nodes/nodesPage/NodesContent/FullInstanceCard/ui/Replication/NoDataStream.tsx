import { Box } from "@mui/material";

import { useTranslation } from "shared/intl";

import { Message } from "./Common";

export const NoDataStream = () => {
  const { translation } = useTranslation();
  const commonTranslation = translation.common;
  return (
    <>
      <Box>-</Box>
      <Message>{commonTranslation.noData}</Message>
    </>
  );
};
