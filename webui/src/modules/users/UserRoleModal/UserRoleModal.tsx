import React, { useState } from "react";
import cn from "classnames";

import { Modal } from "shared/ui/Modal/Modal";
import { Role, User } from "shared/entity/users/types/types";
import { CloseIcon } from "shared/icons/CloseIcon";
import { Button } from "shared/ui/Button/Button";
import { useTranslation } from "shared/intl";

import styles from "./UserRoleModal.module.scss";

export const UserRoleModal = ({
  item,
  onClose,
}: {
  item: Role | User;
  onClose: () => void;
}) => {
  const { translation } = useTranslation();
  const {
    authType,
    roles,
    privilegesRoles,
    noPrivileges,
    selectPrivilege,
    privilegesUsers,
    privilegesForAllUsers,
    privilegesTables,
    privilegesForAllTables,
    close,
  } = translation.pages.users.modal;

  const [selectedUserPrivilege, setSelectedUserPrivilege] = useState<string>();
  const [selectedTablePrivilege, setSelectedTablePrivilege] =
    useState<string>();

  const renderMultiBlock = (
    privileges:
      | {
          type: string;
          items: string[];
          isForAll: boolean;
        }[]
      | null,
    selectedElement: string | undefined,
    setSelectedItem: (v: string) => void,
    forAllItemsTranslate: string
  ) => {
    if (!privileges || privileges.length === 0) {
      return <div className={styles.disabledContainer}>{noPrivileges}</div>;
    }

    const renderUsersMulti = () => {
      if (!selectedElement) {
        return (
          <div className={styles.disabledContainer}>{selectPrivilege}</div>
        );
      }

      const foundPrivilege = privileges?.find(
        (privilege) => privilege.type === selectedElement
      );

      if (!foundPrivilege) return null;

      return foundPrivilege.isForAll ? (
        <div className={styles.disabledContainer}>{forAllItemsTranslate}</div>
      ) : (
        <div className={styles.multiSelect}>
          {foundPrivilege.items.map((elem, i, arr) => {
            return (
              <div className={styles.multiItem} key={i}>
                <div>{elem}</div>
                {i !== arr.length - 1 && <div className={styles.divider} />}
              </div>
            );
          })}
        </div>
      );
    };

    return (
      <>
        <div className={styles.buttonsRow}>
          {privileges.map((privilege, index) => {
            return (
              <div
                key={index}
                className={cn(
                  styles.button,
                  selectedElement === privilege.type && styles.activeButton
                )}
                onClick={() => setSelectedItem(privilege.type)}
              >
                {privilege.type}
              </div>
            );
          })}
        </div>
        {renderUsersMulti()}
      </>
    );
  };

  return (
    <Modal bodyClassName={styles.body}>
      <>
        <div className={styles.titleWrapper}>
          <span className={styles.titleText}>{item.name}</span>
          <div
            className={styles.close}
            onClick={(event) => {
              event.stopPropagation();
              onClose();
            }}
          >
            <CloseIcon />
          </div>
        </div>
        {item.type === "user" && (
          <div className={styles.block}>
            <div className={styles.label}>{authType}</div>
            <div className={styles.value}>{item.authType}</div>
          </div>
        )}
        {item.roles && !!item.roles.length && (
          <div className={styles.block}>
            <div className={styles.label}>{roles}</div>
            <div className={styles.row}>
              {item.roles.map((role, index, arr) => (
                <React.Fragment key={index}>
                  <div className={styles.value}>{role}</div>
                  {index !== arr.length - 1 && (
                    <div className={styles.divider} />
                  )}
                </React.Fragment>
              ))}
            </div>
          </div>
        )}
        <div className={styles.block}>
          <div className={styles.label}>{privilegesRoles}</div>
          {item.privilegesForRoles && !!item.privilegesForRoles.length ? (
            <div className={styles.row}>
              {item.privilegesForRoles.map((role, index, arr) => (
                <React.Fragment key={index}>
                  <div className={styles.value}>{role}</div>
                  {index !== arr.length - 1 && (
                    <div className={styles.divider} />
                  )}
                </React.Fragment>
              ))}
            </div>
          ) : (
            <div className={styles.disabledValue}>{noPrivileges}</div>
          )}
        </div>
        <div className={styles.block}>
          <div className={styles.label}>{privilegesUsers}</div>
          {renderMultiBlock(
            item.privilegesForUsers,
            selectedUserPrivilege,
            setSelectedUserPrivilege,
            privilegesForAllUsers
          )}
        </div>
        <div className={styles.block}>
          <div className={styles.label}>{privilegesTables}</div>
          {renderMultiBlock(
            item.privilegesForTables,
            selectedTablePrivilege,
            setSelectedTablePrivilege,
            privilegesForAllTables
          )}
        </div>
        <div className={styles.footer}>
          <Button theme="secondary" onClick={onClose} size="small">
            {close}
          </Button>
        </div>
      </>
    </Modal>
  );
};
