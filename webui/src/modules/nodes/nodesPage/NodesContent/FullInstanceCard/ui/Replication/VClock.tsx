import { Box, Dialog, styled } from "@mui/material";
import { useState } from "react";

import { useTranslation } from "shared/intl";

import { FlexRow, Label } from "./Common";

const FullListButton = styled(Box)(({ theme }) => ({
  fontSize: 12,
  color: theme.palette.primary.main,
  cursor: "pointer",
  "&: hover": {
    textDecoration: "underline",
  },
}));
const FullListDialogRoot = styled(Box)({
  padding: 10,
  overflow: "auto",
});
type VClockProps = {
  vclock: Record<string, number>;
};
export const VClock = ({ vclock }: VClockProps) => {
  const { translation } = useTranslation();
  const commonTranslation = translation.common;
  const [fullListOpen, setFullListOpen] = useState(false);
  const vclockList = Object.keys(vclock);

  const changeFullListOpen = () => {
    setFullListOpen((flo) => !flo);
  };
  return (
    <Box>
      {vclockList.slice(0, 3).map((key) => {
        return vclock ? (
          <FlexRow gap={1} fontSize={12}>
            <Label>{key} -</Label>
            <Box>{vclock[key]}</Box>
          </FlexRow>
        ) : null;
      })}
      {vclockList.length > 3 ? (
        <>
          <FullListButton onClick={changeFullListOpen}>
            {commonTranslation.showMore}
          </FullListButton>
          <Dialog onClose={changeFullListOpen} open={fullListOpen}>
            <FullListDialogRoot>
              {vclockList.map((key) => {
                return vclock ? (
                  <FlexRow gap={1} fontSize={12}>
                    <Label>{key} -</Label>
                    <Box>{vclock[key]}</Box>
                  </FlexRow>
                ) : null;
              })}
            </FullListDialogRoot>
          </Dialog>
        </>
      ) : null}
    </Box>
  );
};
