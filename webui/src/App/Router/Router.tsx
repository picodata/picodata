import { Navigate, Route, Routes } from "react-router";

import { NodesPage } from "modules/nodes/nodesPage/NodesPage";
import { URL_CONFIG } from "shared/router/config";
import { UsersPage } from "modules/users/UsersPage";

export const Router = () => (
  <Routes>
    <Route path={URL_CONFIG.NODES.path} Component={NodesPage} />
    <Route path={URL_CONFIG.USERS.path} Component={UsersPage} />
    <Route path="*" element={<Navigate to={URL_CONFIG.NODES.path} />} />
  </Routes>
);
