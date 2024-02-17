import ReactDOM from "react-dom/client";

import App from "./App/App";
import { IntlProvider } from "./providers/IntlProvider";
import { ApiProvider } from "./providers/ApiProvider";

import "./styles/global.scss";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <IntlProvider>
    <ApiProvider>
      <App />
    </ApiProvider>
  </IntlProvider>
);
