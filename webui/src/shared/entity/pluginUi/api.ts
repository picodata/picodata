import { AxiosInstance } from "axios";

import { ServerPluginUiPagesListType } from "./types";
import { GET_PLUGIN_UI_PAGES_URL } from "./constants";

export const getPluginUiPages = async (
  axios: AxiosInstance,
  signal?: AbortSignal
) => {
  const response = await axios.get<ServerPluginUiPagesListType>(
    GET_PLUGIN_UI_PAGES_URL,
    { signal }
  );

  return response.data;
};
