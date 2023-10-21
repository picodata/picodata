import { Navigate, Route, Routes } from "react-router";

import { ReplicasetsPage } from "modules/replicaset/replicasetsPage/ReplicasetsPage";
import { URL_CONFIG } from "shared/router/config";

export const Router = () => (
  <Routes>
    <Route path={URL_CONFIG.NODES.path} Component={ReplicasetsPage} />
    <Route path="*" element={<Navigate to={URL_CONFIG.NODES.path} />} />
  </Routes>
);
