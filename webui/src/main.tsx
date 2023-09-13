import ReactDOM from "react-dom/client";
import App from "./App";
import { store } from "./store/index";
import { Provider } from "react-redux";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <Provider store={store}>
    <App />
  </Provider>
);
