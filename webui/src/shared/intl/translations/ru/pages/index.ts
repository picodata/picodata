import { TPages } from "./types";
import { instances } from "./instances";
import { users } from "./users";
import { login } from "./login";
import { pluginUi } from "./pluginUi";

export const pages: TPages = {
  pluginUi,
  instances,
  users,
  login,
};
