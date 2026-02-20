import "shared/entity/config/api";
import ReactDOM from "react-dom/client";

import App from "./App";
import { AppProviders } from "./providers";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <AppProviders>
    <App />
  </AppProviders>
);
