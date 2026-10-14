import type {
  Unsubscribe,
  WebuiPageElement,
  WebuiSdk,
  WebuiSessionError,
  WebuiSessionErrorCode,
  WebuiTheme,
} from "@picodata/ui-kit";
import { tokens, type Tokens } from "@picodata/ui-kit/tokens";

const palette: Tokens["palette"] = tokens.palette;
const font: string = tokens.common.variables.fontFamily;
const radius: number = tokens.shape.borderRadius;
void [palette, font, radius];

// @ts-expect-error Published token types are readonly.
tokens.palette.primary.main = "red";

export default class PluginPage
  extends HTMLElement
  implements WebuiPageElement
{
  webuiSdk!: WebuiSdk;
  private unsubscribe?: Unsubscribe;

  connectedCallback() {
    const sdk = this.webuiSdk;
    const version: string = sdk.version;
    const locale: string = sdk.getLocale();
    const theme: WebuiTheme = sdk.theme;
    const token: string = sdk.getSessionToken();
    const url: string = sdk.apiBaseUrl;
    const stopTheme = sdk.onThemeChange((nextTheme: WebuiTheme) => {
      this.dataset.theme = nextTheme;
    });
    const stopLocale = sdk.onLocaleChange((nextLocale: string) => {
      this.lang = nextLocale;
    });
    this.unsubscribe = () => {
      stopTheme();
      stopLocale();
    };
    const refreshed: Promise<string> = sdk.refreshSession();
    void refreshed.catch((error: WebuiSessionError) => {
      const code: WebuiSessionErrorCode = error.code;
      sdk.reportFatalError(new Error(`${code}: ${error.message}`));
    });
    void [version, locale, theme, token, url];

    // @ts-expect-error The host owns the current theme.
    sdk.theme = "dark";
    // @ts-expect-error Token access is synchronous.
    const pending: Promise<string> = sdk.getSessionToken();
    void pending;
  }

  disconnectedCallback() {
    this.unsubscribe?.();
  }
}
