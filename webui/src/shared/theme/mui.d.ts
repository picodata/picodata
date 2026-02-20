import "@mui/material/Button";
import "@mui/material/styles";

declare module "@mui/material/styles" {
  interface Theme {
    additionalPalette: {
      colorTextOrange: "#f7941d";
    };
    common: {
      colors: {
        typography: {
          colorTextBlack: string;
          colorTextGrey: string;
          colorTextOrange: string;
          $colorTextDisabled: string;
        };
        bg: {
          colorBgWhite: string;
          colorBgNormal: string;
          colorBgLight: string;
          colorBgGray: string;
          colorBgGrey: string;
          colorBgBeige: string;
          colorBgTabs: string;
          colorBgTableLine: string;
          colorBgLightBeige: string;
        };
        bgRed: {
          colorBgRed: string;
        };
        buttons: {
          colorButtonText: string;
        };
        fields: {
          colorFieldForm: string;
          colorFieldText: string;
          colorFieldTextMain: string;
          colorFieldTextNormal: string;
          colorFieldDisabled: string;
          colorFieldStroke: string;
          colorFieldStrokeNormal: string;
        };
        primary: {
          colorPrimary: string;
          colorPrimaryHover: string;
          colorPrimarySelected: string;
          colorPrimaryDisabled: string;
        };
        secondary: {
          colorSecondary: string;
          colorSecondaryHover: string;
          colorSecondarySelected: string;
          colorSecondaryDisabled: string;
        };
        grey: {
          colorLightGrey: string;
        };
        notifications: {
          colorNotificationSuccess: string;
        };
        disabled: {
          colorDisabled: string;
          colorIconDisabled: string;
        };
      };
      variables: {
        transitionNormal: string;
        dropdownOffset: string;
        elevation1: string;
        elevationBg: string;
        sideMenuWidth: string;
      };
      zIndex: {
        zIndexDropdown: number;
        zIndexModal: number;
        zIndexTooltip: number;
        zIndexSideMenu: number;
      };
    };
  }
  interface ThemeOptions {
    additionalPalette?: {
      colorTextOrange?: "#f7941d";
    };
    common?: {
      colors?: {
        typography?: {
          colorTextBlack?: string;
          colorTextGrey?: string;
          colorTextOrange?: string;
          $colorTextDisabled?: string;
        };
        bg?: {
          colorBgWhite?: string;
          colorBgNormal?: string;
          colorBgLight?: string;
          colorBgGray?: string;
          colorBgGrey?: string;
          colorBgBeige?: string;
          colorBgTabs?: string;
          colorBgTableLine?: string;
          colorBgLightBeige?: string;
        };
        bgRed?: {
          colorBgRed?: string;
        };
        buttons?: {
          colorButtonText?: string;
        };
        fields?: {
          colorFieldForm?: string;
          colorFieldText?: string;
          colorFieldTextMain?: string;
          colorFieldTextNormal?: string;
          colorFieldDisabled?: string;
          colorFieldStroke?: string;
          colorFieldStrokeNormal?: string;
        };
        primary?: {
          colorPrimary?: string;
          colorPrimaryHover?: string;
          colorPrimarySelected?: string;
          colorPrimaryDisabled?: string;
        };
        secondary?: {
          colorSecondary?: string;
          colorSecondaryHover?: string;
          colorSecondarySelected?: string;
          colorSecondaryDisabled?: string;
        };
        grey?: {
          colorLightGrey?: string;
        };
        notifications?: {
          colorNotificationSuccess?: string;
        };
        disabled?: {
          colorDisabled?: string;
          colorIconDisabled?: string;
        };
      };
      variables?: {
        transitionNormal?: string;
        dropdownOffset?: string;
        elevation1?: string;
        elevationBg?: string;
        sideMenuWidth?: string;
      };
      zIndex: {
        zIndexDropdown?: number;
        zIndexModal?: number;
        zIndexTooltip?: number;
        zIndexSideMenu?: number;
      };
    };
  }
}
