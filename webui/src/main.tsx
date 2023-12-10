import ReactDOM from "react-dom/client";

import App from "./App/App";
import { ApiProvider } from "./providers/ApiProvider";

import "./styles/global.scss";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <ApiProvider>
    <App />
  </ApiProvider>
);
