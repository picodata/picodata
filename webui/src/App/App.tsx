import { BrowserRouter } from "react-router-dom";

import { Router } from "./Router/Router";
import { Header } from "./Header/Header";
import { SideMenu } from "./SideMenu/SideMenu";

import styles from "./App.module.scss";

function App() {
  return (
    <div className={styles.container}>
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
