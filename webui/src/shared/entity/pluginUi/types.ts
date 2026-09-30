export type ServerPluginUiPageType = {
  plugin: string;
  slug: string;
  title: string;
  entry: string;
  version: string;
  auth: "cluster" | "plugin";
};

export type ServerPluginUiPagesListType = ServerPluginUiPageType[];
