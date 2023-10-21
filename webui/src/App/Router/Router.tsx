import { Route, Routes } from "react-router";

import { ReplicasetsPage } from "modules/replicaset/replicasetsPage/ReplicasetsPage";

export const Router = () => (
  <Routes>
    <Route path="/" Component={ReplicasetsPage} />
  </Routes>
);
