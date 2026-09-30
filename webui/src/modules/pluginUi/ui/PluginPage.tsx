import { Component, ErrorInfo, ReactNode, useState } from "react";
import { Button, styled, Typography } from "@mui/material";

import { PluginUiPageType } from "shared/entity/pluginUi";
import { useTranslation } from "shared/intl";
import { LoadingIndicator } from "shared/ui/LoadingIndicator";

import { usePluginPage } from "../models";
import { logPluginPageError } from "../utils";

export const PageContent = styled("section")({
  display: "flex",
  flexDirection: "column",
  alignItems: "flex-start",
  gap: "16px",
  paddingTop: "32px",
});
const MountPoint = styled("div")({
  width: "100%",
  height: "100%",
  overflow: "auto",
  "& > *": { display: "block" },
});

function Failure({ retry }: { retry: () => void }) {
  const text = useTranslation().translation.pages.pluginUi;
  return (
    <PageContent role="alert">
      <Typography>{text.mountError}</Typography>
      <Button onClick={retry} variant={"outlined"}>
        {text.retry}
      </Button>
    </PageContent>
  );
}

class PageBoundary extends Component<
  { children: ReactNode; retry: () => void; page: PluginUiPageType },
  { failed: boolean }
> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    logPluginPageError(
      this.props.page,
      "render",
      error,
      info.componentStack ?? undefined
    );
  }

  render() {
    return this.state.failed ? (
      <Failure retry={this.props.retry} />
    ) : (
      this.props.children
    );
  }
}

function MountedPage({
  page,
  retry,
}: {
  page: PluginUiPageType;
  retry: () => void;
}) {
  const { container, failed, loading } = usePluginPage(page);
  if (failed) return <Failure retry={retry} />;
  return (
    <>
      {loading && <LoadingIndicator />}
      <MountPoint ref={container} />
    </>
  );
}

export function PluginPage({ page }: { page: PluginUiPageType }) {
  const [attempt, setAttempt] = useState(0);
  const retry = () => setAttempt((value) => value + 1);
  return (
    <PageBoundary key={attempt} retry={retry} page={page}>
      <MountedPage page={page} retry={retry} />
    </PageBoundary>
  );
}
