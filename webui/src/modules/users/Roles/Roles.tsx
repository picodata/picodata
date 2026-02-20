import { Role } from "shared/entity/users/types/types";
import { useRolesInfoQuery } from "shared/entity/users/api/useRolesInfoQuery";

import { UserRoleCard } from "../UserRoleCard/UserRoleCard";

export const Roles = ({
  setSelectedItem,
  search,
}: {
  setSelectedItem: (v: Role) => void;
  search: string;
}) => {
  const { data } = useRolesInfoQuery();

  return (
    <>
      {(data as Array<Role>) //ToDo types
        ?.filter((role) => {
          if (search.length) {
            return role.name
              .toLocaleLowerCase()
              .includes(search.toLocaleLowerCase());
          }

          return true;
        })
        .map((item, i) => {
          return (
            <UserRoleCard
              key={i}
              // className={styles.item} ToDo
              type="ROLES"
              onClick={() => setSelectedItem(item)}
              card={item}
            />
          );
        })}
    </>
  );
};
