import { useUsersInfoQuery } from "shared/entity/users/api/useUsersInfoQuery";
import { User } from "shared/entity/users/types/types";

import { UserRoleCard } from "../UserRoleCard/UserRoleCard";

export const Users = ({
  setSelectedItem,
  search,
}: {
  setSelectedItem: (v: User) => void;
  search: string;
}) => {
  const { data } = useUsersInfoQuery();

  return (
    <>
      {(data as Array<User>) //ToDo types
        ?.filter((user) => {
          if (search.length) {
            return user.name
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
              type="USERS"
              onClick={() => setSelectedItem(item)}
              card={item}
            />
          );
        })}
    </>
  );
};
