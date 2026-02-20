import React, { FC } from "react";

import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";
import { useTranslation } from "shared/intl";

import {
  Content,
  InfoValue,
  Label,
  NameColumn,
  PrivilegesForRoles,
  PrivilegesForTables,
  PrivilegesForUsers,
  RoleColumn,
  Root,
  textSx,
} from "./StyledComponents";

export interface UserRoleProps {
  type: "USERS" | "ROLES";
  // className?: string; ToDo
  onClick: () => void;
  card: {
    name: string;
    roles: string[] | null;
    privilegesForRoles: string[] | null;
    privilegesForUsers: Array<{
      type: string;
      items: string[];
      isForAll: boolean;
    }> | null;
    privilegesForTables: Array<{
      type: string;
      items: string[];
      isForAll: boolean;
    }> | null;
  };
}

export const UserRoleCard: FC<UserRoleProps> = React.memo(
  ({
    type,
    card,
    // className, ToDo
    onClick,
  }) => {
    const { translation } = useTranslation();
    const {
      userName,
      roleName,
      roles,
      privilegesForUser,
      privilegesForRoles,
      privilegesForTables,
    } = translation.pages.users;

    return (
      <Root
        // className={cn(styles.cardWrapper, className)} ToDo
        onClick={onClick}
      >
        <Content>
          <NameColumn>
            <Label>{type === "USERS" ? userName : roleName}</Label>
            <InfoValue>
              <HiddenWrapper place="bottom" sx={textSx}>
                {card.name}
              </HiddenWrapper>
            </InfoValue>
          </NameColumn>
          <RoleColumn>
            <Label>{roles}</Label>
            <InfoValue>
              <HiddenWrapper place="bottom" sx={textSx}>
                {card.roles && !!card.roles.length
                  ? card.roles.join(", ")
                  : "-"}
              </HiddenWrapper>
            </InfoValue>
          </RoleColumn>
          <PrivilegesForUsers>
            <Label>{privilegesForUser}</Label>
            <InfoValue>
              <HiddenWrapper place="bottom" sx={textSx}>
                {card.privilegesForUsers && !!card.privilegesForUsers.length
                  ? card.privilegesForUsers
                      .map((p) => `${p.type} user`)
                      .join(", ")
                  : "-"}
              </HiddenWrapper>
            </InfoValue>
          </PrivilegesForUsers>
          <PrivilegesForRoles>
            <Label>{privilegesForRoles}</Label>
            <InfoValue>
              <HiddenWrapper place="bottom" sx={textSx}>
                {card.privilegesForRoles && !!card.privilegesForRoles.length
                  ? card.privilegesForRoles.join(", ")
                  : "-"}
              </HiddenWrapper>
            </InfoValue>
          </PrivilegesForRoles>
          <PrivilegesForTables>
            <Label>{privilegesForTables}</Label>
            <InfoValue>
              <HiddenWrapper place="bottom" sx={textSx}>
                {card.privilegesForTables && !!card.privilegesForTables.length
                  ? card.privilegesForTables
                      .map((p) => `${p.type} table`)
                      .join(", ")
                  : "-"}
              </HiddenWrapper>
            </InfoValue>
          </PrivilegesForTables>
        </Content>
      </Root>
    );
  }
);
