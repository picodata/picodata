import { ThemeProvider } from "../shared/theme";

import { BrowserRouter } from "./Router";

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter />
    </ThemeProvider>
  );
}

export default App;
