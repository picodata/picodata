import React, { FC } from "react";
import cn from "classnames";

import { HiddenWrapper } from "shared/ui/HiddenWrapper/HiddenWrapper";

import styles from "./UserRoleCard.module.scss";

export interface UserRoleProps {
  type: "USERS" | "ROLES";
  className?: string;
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
  ({ type, card, className, onClick }) => {
    const userName = "Имя пользователя";
    const roleName = "Название роли";
    const roles = "Роли";
    const privilegesForUser = "Привилегии для пользователей";
    const privilegesForRoles = "Привилегии для ролей";
    const privilegesForTables = "Привилегии для таблиц";

    return (
      <div className={cn(styles.cardWrapper, className)} onClick={onClick}>
        <div className={styles.content}>
          <div className={cn(styles.nameColumn)}>
            <div className={styles.label}>
              {type === "USERS" ? userName : roleName}
            </div>
            <div className={styles.infoValue}>
              <HiddenWrapper place="bottom" className={styles.text}>
                {card.name}
              </HiddenWrapper>
            </div>
          </div>
          <div className={styles.roleColumn}>
            <div className={styles.label}>{roles}</div>
            <div className={styles.infoValue}>
              <HiddenWrapper place="bottom" className={styles.text}>
                {card.roles && !!card.roles.length
                  ? card.roles.join(", ")
                  : "-"}
              </HiddenWrapper>
            </div>
          </div>
          <div className={styles.privilegesForUsers}>
            <div className={styles.label}>{privilegesForUser}</div>
            <div className={styles.infoValue}>
              <HiddenWrapper place="bottom" className={styles.text}>
                {card.privilegesForUsers && !!card.privilegesForUsers.length
                  ? card.privilegesForUsers
                      .map((p) => `${p.type} user`)
                      .join(", ")
                  : "-"}
              </HiddenWrapper>
            </div>
          </div>
          <div className={styles.privilegesForRoles}>
            <div className={styles.label}>{privilegesForRoles}</div>
            <div className={styles.infoValue}>
              <HiddenWrapper place="bottom" className={styles.text}>
                {card.privilegesForRoles && !!card.privilegesForRoles.length
                  ? card.privilegesForRoles.join(", ")
                  : "-"}
              </HiddenWrapper>
            </div>
          </div>
          <div className={styles.privilegesForTables}>
            <div className={styles.label}>{privilegesForTables}</div>
            <div className={styles.infoValue}>
              <HiddenWrapper place="bottom" className={styles.text}>
                {card.privilegesForTables && !!card.privilegesForTables.length
                  ? card.privilegesForTables
                      .map((p) => `${p.type} table`)
                      .join(", ")
                  : "-"}
              </HiddenWrapper>
            </div>
          </div>
        </div>
      </div>
    );
  }
);
