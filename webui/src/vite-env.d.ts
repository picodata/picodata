/// <reference types="vite/client" />
/// <reference types="vite-plugin-svgr/client" />

interface ImportMetaEnv {
  readonly VITE_DOCS_BASE_URL: string;
  readonly VITE_DOCS_CONFIG_PATH: string;
  readonly VITE_DOCS_GLOSSARY_PATH: string;
  readonly VITE_DOCS_ARCHITECTURE_RAFT_FAILOVER_PATH: string;
  readonly VITE_DOCS_VERSION: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
