import { useParams } from "react-router-dom";
import { Button, Typography } from "@mui/material";

import { usePluginUiPages } from "shared/entity/pluginUi";
import { getPluginUiPageKey } from "shared/entity/pluginUi/utils";
import { useTranslation } from "shared/intl";
import { LoadingIndicator } from "shared/ui/LoadingIndicator";
import { PluginPage, PageContent } from "modules/pluginUi/ui";

import { Protected } from "./Protected";

function PluginPageRoute() {
  const { plugin, slug } = useParams();
  const pages = usePluginUiPages();
  const text = useTranslation().translation.pages.pluginUi;

  if (pages.isLoading) return <LoadingIndicator />;
  if (pages.isError && !pages.data) {
    return (
      <PageContent role="alert">
        <Typography variant="h5">{text.loadError}</Typography>
        <Button
          disabled={pages.isFetching}
          onClick={() => void pages.refetch()}
        >
          {text.retry}
        </Button>
      </PageContent>
    );
  }

  const page = pages.data?.find(
    (item) => item.plugin === plugin && item.slug === slug
  );
  if (!page) {
    return (
      <PageContent role="status">
        <Typography variant="h5">{text.unavailable}</Typography>
        <Typography>{text.unavailableDescription}</Typography>
      </PageContent>
    );
  }

  const content = <PluginPage key={getPluginUiPageKey(page)} page={page} />;
  return page.auth === "cluster" ? <Protected>{content}</Protected> : content;
}

export { PluginPageRoute as Component };
