import { styled, SxProps } from "@mui/material";

import { PageContainer } from "../../shared/ui/layout/PageContainer";

export const containerSx: SxProps = {
  padding: "16px 24px",
};

export const Items = styled("div")({
  marginTop: "20px",
});

export const UserPagContainer = styled(PageContainer)({
  display: "flex",
  flexDirection: "column",
  flexGrow: 1,
});
