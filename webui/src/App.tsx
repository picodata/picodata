import { BrowserRouter } from "react-router-dom";
import { Header } from "./components/header/Header";
import { Router } from "./components/router/Router";
import styles from "./App.module.css";
import { SideMenu } from "./components/sideMenu/SideMenu";

function App() {
  return (
    <div className={styles.layout}>
      <Header />
      <BrowserRouter>
        <main className={styles.layoutMain}>
          <SideMenu />
          <div className={styles.bodyWrapper}>
            <Router />
          </div>
        </main>
      </BrowserRouter>
    </div>
  );
}

export default App;
