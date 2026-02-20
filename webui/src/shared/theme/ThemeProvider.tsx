import { PropsWithChildren } from "react";
import { ThemeProvider as MuiThemeProvider, CssBaseline } from "@mui/material";

import { GlobalStyles } from "./GlobalStyles";
import { theme } from "./theme";
import "@fontsource/roboto/300.css";
import "@fontsource/roboto/400.css";
import "@fontsource/roboto/500.css";
import "@fontsource/roboto/700.css";

export const ThemeProvider = ({ children }: PropsWithChildren) => {
  return (
    <MuiThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles />
      {children}
    </MuiThemeProvider>
  );
};
