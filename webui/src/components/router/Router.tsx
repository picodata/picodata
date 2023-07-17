import { ReplicasetsPage } from "../replicasetsPage/ReplicasetsPage";
import { Route, Routes } from "react-router";

export const Router = () => (
  <Routes>
    <Route path="/" Component={ReplicasetsPage} />
  </Routes>
);
