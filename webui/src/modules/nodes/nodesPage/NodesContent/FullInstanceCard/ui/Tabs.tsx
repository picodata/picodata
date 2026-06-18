import {
  Tabs as MuiTabs,
  Tab as MuiTab,
  TabsProps as MuiTabsProps,
  styled,
} from "@mui/material";
import { Dispatch, SetStateAction } from "react";
import { grey } from "@mui/material/colors";

import { useTranslation } from "shared/intl";

import { Tab } from "../model";
import { getTabTranslation, tabList } from "../lib";

const StyledTab = styled(MuiTab)({
  textTransform: "none",
  minWidth: "auto",
  width: "auto",
  px: 0,
  paddingRight: 20,
  fontWeight: "normal",
});

const StyledMuiTabs = styled(MuiTabs)({
  borderBottom: `1px solid ${grey[300]}`,
  paddingLeft: 20,
});

type TabsProps = {
  setTab: Dispatch<SetStateAction<number>>;
  tab: number;
  replicaDisabled: boolean;
};

export const Tabs = ({ tab, setTab, replicaDisabled }: TabsProps) => {
  const t = useTranslation();
  const tabsChangeHandler: MuiTabsProps["onChange"] = (_, value) => {
    setTab(value);
  };

  return (
    <StyledMuiTabs
      variant="scrollable"
      value={tab}
      onChange={tabsChangeHandler}
    >
      {tabList.map((key) => (
        <StyledTab
          key={key}
          id={key}
          label={getTabTranslation(key, t)}
          disabled={key === Tab.Replica && replicaDisabled}
        />
      ))}
    </StyledMuiTabs>
  );
};
