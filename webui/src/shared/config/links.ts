export const docsLinks = {
  base: import.meta.env.VITE_DOCS_BASE_URL,
  configPath: import.meta.env.VITE_DOCS_CONFIG_PATH,
  glossaryPath: import.meta.env.VITE_DOCS_GLOSSARY_PATH,
  achitectureRaftFailoverPath: import.meta.env
    .VITE_DOCS_ARCHITECTURE_RAFT_FAILOVER_PATH,
  version: import.meta.env.VITE_DOCS_VERSION,
} as const;

export const getConfigurationLink = (anchor: string) => {
  return `${docsLinks.base}${docsLinks.version}/${docsLinks.configPath}${anchor}`;
};
export const getGlossaryLink = (anchor: string) => {
  return `${docsLinks.base}${docsLinks.version}/${docsLinks.glossaryPath}${anchor}`;
};
export const getRaftFailoverLink = (anchor: string) => {
  return `${docsLinks.base}${docsLinks.version}/${docsLinks.achitectureRaftFailoverPath}${anchor}`;
};
