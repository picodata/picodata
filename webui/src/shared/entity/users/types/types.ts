export type User = {
  name: string;
  type: "user";
  authType: string;
  roles: string[];
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

export type Role = {
  name: string;
  type: "role";
  roles: string[];
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
