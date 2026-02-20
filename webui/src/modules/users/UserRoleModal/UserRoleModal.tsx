import React, { useState } from "react";

import { Modal } from "shared/ui/Modal/Modal";
import { Role, User } from "shared/entity/users/types/types";
import { CloseIcon } from "shared/icons/CloseIcon";
import { Button } from "shared/ui/Button/Button";
import { useTranslation } from "shared/intl";

import {
  BlockElement,
  ButtonElement,
  ButtonsRow,
  CloseElement,
  DisabledContainer,
  DisabledValue,
  Divider,
  Footer,
  Label,
  modalBodySx,
  MultiItem,
  MultiSelect,
  Row,
  TitleText,
  TitleWrapper,
  Value,
} from "./StyledComponents";

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
      return <DisabledContainer>{noPrivileges}</DisabledContainer>;
    }

    const renderUsersMulti = () => {
      if (!selectedElement) {
        return <DisabledContainer>{selectPrivilege}</DisabledContainer>;
      }

      const foundPrivilege = privileges?.find(
        (privilege) => privilege.type === selectedElement
      );

      if (!foundPrivilege) return null;

      return foundPrivilege.isForAll ? (
        <DisabledContainer>{forAllItemsTranslate}</DisabledContainer>
      ) : (
        <MultiSelect>
          {foundPrivilege.items.map((elem, i, arr) => {
            return (
              <MultiItem key={i}>
                <div>{elem}</div>
                {i !== arr.length - 1 && <Divider />}
              </MultiItem>
            );
          })}
        </MultiSelect>
      );
    };

    return (
      <>
        <ButtonsRow>
          {privileges.map((privilege, index) => {
            return (
              <ButtonElement
                key={index}
                $isActive={selectedElement === privilege.type}
                onClick={() => setSelectedItem(privilege.type)}
              >
                {privilege.type}
              </ButtonElement>
            );
          })}
        </ButtonsRow>
        {renderUsersMulti()}
      </>
    );
  };

  return (
    <Modal bodySx={modalBodySx}>
      <>
        <TitleWrapper>
          <TitleText>{item.name}</TitleText>
          <CloseElement
            onClick={(event) => {
              event.stopPropagation();
              onClose();
            }}
          >
            <CloseIcon />
          </CloseElement>
        </TitleWrapper>
        {item.type === "user" && (
          <BlockElement>
            <Label>{authType}</Label>
            <Value>{item.authType}</Value>
          </BlockElement>
        )}
        {item.roles && !!item.roles.length && (
          <BlockElement>
            <Label>{roles}</Label>
            <Row>
              {item.roles.map((role, index, arr) => (
                <React.Fragment key={index}>
                  <Value>{role}</Value>
                  {index !== arr.length - 1 && <Divider />}
                </React.Fragment>
              ))}
            </Row>
          </BlockElement>
        )}
        <BlockElement>
          <Label>{privilegesRoles}</Label>
          {item.privilegesForRoles && !!item.privilegesForRoles.length ? (
            <Row>
              {item.privilegesForRoles.map((role, index, arr) => (
                <React.Fragment key={index}>
                  <Value>{role}</Value>
                  {index !== arr.length - 1 && <Divider />}
                </React.Fragment>
              ))}
            </Row>
          ) : (
            <DisabledValue>{noPrivileges}</DisabledValue>
          )}
        </BlockElement>
        <BlockElement>
          <Label>{privilegesUsers}</Label>
          {renderMultiBlock(
            item.privilegesForUsers,
            selectedUserPrivilege,
            setSelectedUserPrivilege,
            privilegesForAllUsers
          )}
        </BlockElement>
        <BlockElement>
          <Label>{privilegesTables}</Label>
          {renderMultiBlock(
            item.privilegesForTables,
            selectedTablePrivilege,
            setSelectedTablePrivilege,
            privilegesForAllTables
          )}
        </BlockElement>
        <Footer>
          <Button theme="secondary" onClick={onClose} size="small">
            {close}
          </Button>
        </Footer>
      </>
    </Modal>
  );
};
