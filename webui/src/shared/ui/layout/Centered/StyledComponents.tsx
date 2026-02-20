import { styled } from "@mui/material";

export const Root = styled("div")(({ theme }) => ({
  position: "relative",
  backgroundColor: theme.common.colors.bg.colorBgLightBeige,
  width: "100vw",
  height: "100vh",
  display: "grid",
  gridTemplateColumns: "1fr clamp(32.5%, 468px, 90%) 1fr",
  gridTemplateRows: "1fr auto 1fr",
  gridTemplateAreas: `
    ". . ."
    ". main ."
    "info info ."
  `,
}));
