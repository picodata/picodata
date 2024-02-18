import cn from "classnames";
import { useState } from "react";

import { Content } from "shared/ui/layout/Content/Content";
import { PageContainer } from "shared/ui/layout/PageContainer/PageContainer";
import { Role, User } from "shared/entity/users/types/types";

import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { TopBar } from "./TopBar/TopBar";
import { UserRoleModal } from "./UserRoleModal/UserRoleModal";
import { Roles } from "./Roles/Roles";
import { Users } from "./Users/Users";

import styles from "./UsersPage.module.scss";

export const UsersPage = () => {
  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();
  const [selectedItem, setSelectedItem] = useState<Role | User>();
  const [search, setSearch] = useState("");

  const renderModal = () => {
    if (!selectedItem) {
      return null;
    }

    return (
      <UserRoleModal
        item={selectedItem}
        onClose={() => setSelectedItem(undefined)}
      />
    );
  };

  return (
    <PageContainer>
      <Content className={cn(styles.container)}>
        <TopBar
          search={search}
          setSearch={setSearch}
          setGroupByFilterValue={setGroupByFilterValue}
          groupByFilterValue={groupByFilterValue}
        />
        <div className={styles.items}>
          {groupByFilterValue && (
            <>
              {groupByFilterValue === "USERS" && (
                <Users setSelectedItem={setSelectedItem} search={search} />
              )}
              {groupByFilterValue === "ROLES" && (
                <Roles setSelectedItem={setSelectedItem} search={search} />
              )}
            </>
          )}
        </div>
        {renderModal()}
      </Content>
    </PageContainer>
  );
};
