import ReactDOM from "react-dom/client";
import { Provider } from "react-redux";

import App from "./App/App";
import { store } from "./store/index";
import "./styles/global.scss";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <Provider store={store}>
    <App />
  </Provider>
);
