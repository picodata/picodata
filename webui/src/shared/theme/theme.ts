import { createTheme } from "@mui/material";

export const theme = createTheme({
  palette: {
    background: {
      default: "#f3eee9",
    },
    text: {
      primary: "#050505",
      secondary: "#848484",
      disabled: "#c5c0db",
    },
    common: {
      black: "#050505",
      white: "#ffffff",
    },
    primary: {
      main: "#f7ad4e",
      light: "#ffba79",
      dark: "#dd7900",
      contrastText: "#f9f5f2",
    },
    secondary: {
      main: "#DD4A4A",
      light: "#e8ebf1",
      dark: "#d0d4db",
      contrastText: "#050505",
    },
  },
  common: {
    colors: {
      typography: {
        colorTextBlack: "#050505",
        colorTextGrey: "#848484",
        colorTextOrange: "#f7941d",
        $colorTextDisabled: "#c5c0db",
      },
      bg: {
        colorBgWhite: "#ffffff",
        colorBgNormal: "#f7ad4e",
        colorBgLight: "#f3eee9",
        colorBgGray: "#323232",
        colorBgGrey: "#e8e8e9",
        colorBgBeige: "#ede9e3",
        colorBgTabs: "#fdfdfd",
        colorBgTableLine: "#f5f5f3",
        colorBgLightBeige: "#F7F4F1",
      },
      bgRed: {
        colorBgRed: "#DD4A4A",
      },
      buttons: {
        colorButtonText: "#050505",
      },
      fields: {
        colorFieldForm: "#f9f5f2",
        colorFieldText: "#1f1f1f",
        colorFieldTextMain: "#060606",
        colorFieldTextNormal: "#848484",
        colorFieldDisabled: "#f8f9fb",
        colorFieldStroke: "#4f4f4f",
        colorFieldStrokeNormal: "#cecdcd",
      },
      primary: {
        colorPrimary: "#f7ad4e",
        colorPrimaryHover: "#f7941d",
        colorPrimarySelected: "#dd7900",
        colorPrimaryDisabled: "#ffba79",
      },
      secondary: {
        colorSecondary: "#e8ebf1",
        colorSecondaryHover: "#e8ebf1",
        colorSecondarySelected: "#d0d4db",
        colorSecondaryDisabled: "#e8ebf1",
      },
      grey: {
        colorLightGrey: "#efeef2",
      },
      notifications: {
        colorNotificationSuccess: "#c0ecd4",
      },
      disabled: {
        colorDisabled: "#E6E6E6",
        colorIconDisabled: "#848484",
      },
    },
    variables: {
      transitionNormal: "0.2s ease-out",
      dropdownOffset: "2px",

      elevation1: `
        0px 2px 4px 0px rgba(63, 65, 68, 0.12),
        0px 0px 2px 0px rgba(63, 65, 68, 0.12)
      `,
      elevationBg: `
        0px 6px 8px 0px rgba(131, 131, 131, 0.12),
        0px 0px 4px 0px rgba(131, 131, 131, 0.4)
      `,
      sideMenuWidth: "112px",
    },
    zIndex: {
      zIndexDropdown: 10,
      zIndexModal: 20,
      zIndexTooltip: 20,
      zIndexSideMenu: 2,
    },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: `
        /* cyrillic-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 400;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic-ext.woff2) format('woff2');
          unicode-range: U+0460-052F, U+1C80-1C8A, U+20B4, U+2DE0-2DFF, U+A640-A69F, U+FE2E-FE2F;
        }
        /* cyrillic */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 400;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic.woff2) format('woff2');
          unicode-range: U+0301, U+0400-045F, U+0490-0491, U+04B0-04B1, U+2116;
        }
        /* latin-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 400;
          font-display: swap;
          src: url(/fonts/montserrat/latin-ext.woff2) format('woff2');
          unicode-range: U+0100-02BA, U+02BD-02C5, U+02C7-02CC, U+02CE-02D7, U+02DD-02FF, U+0304, U+0308, U+0329, U+1D00-1DBF, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, U+2C60-2C7F, U+A720-A7FF;
        }
        /* latin */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 400;
          font-display: swap;
          src: url(/fonts/montserrat/latin.woff2) format('woff2');
          unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD;
        }
        /* cyrillic-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 500;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic-ext.woff2) format('woff2');
          unicode-range: U+0460-052F, U+1C80-1C8A, U+20B4, U+2DE0-2DFF, U+A640-A69F, U+FE2E-FE2F;
        }
        /* cyrillic */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 500;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic.woff2) format('woff2');
          unicode-range: U+0301, U+0400-045F, U+0490-0491, U+04B0-04B1, U+2116;
        }
        /* latin-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 500;
          font-display: swap;
          src: url(/fonts/montserrat/latin-ext.woff2) format('woff2');
          unicode-range: U+0100-02BA, U+02BD-02C5, U+02C7-02CC, U+02CE-02D7, U+02DD-02FF, U+0304, U+0308, U+0329, U+1D00-1DBF, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, U+2C60-2C7F, U+A720-A7FF;
        }
        /* latin */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 500;
          font-display: swap;
          src: url(/fonts/montserrat/latin.woff2) format('woff2');
          unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD;
        }
        /* cyrillic-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 700;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic-ext.woff2) format('woff2');
          unicode-range: U+0460-052F, U+1C80-1C8A, U+20B4, U+2DE0-2DFF, U+A640-A69F, U+FE2E-FE2F;
        }
        /* cyrillic */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 700;
          font-display: swap;
          src: url(/fonts/montserrat/cyrillic.woff2) format('woff2');
          unicode-range: U+0301, U+0400-045F, U+0490-0491, U+04B0-04B1, U+2116;
        }
        /* latin-ext */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 700;
          font-display: swap;
          src: url(/fonts/montserrat/latin-ext.woff2) format('woff2');
          unicode-range: U+0100-02BA, U+02BD-02C5, U+02C7-02CC, U+02CE-02D7, U+02DD-02FF, U+0304, U+0308, U+0329, U+1D00-1DBF, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, U+2C60-2C7F, U+A720-A7FF;
        }
        /* latin */
        @font-face {
          font-family: 'Montserrat';
          font-style: normal;
          font-weight: 700;
          font-display: swap;
          src: url(/fonts/montserrat/latin.woff2) format('woff2');
          unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD;
        }
      `,
    },
  },
});
