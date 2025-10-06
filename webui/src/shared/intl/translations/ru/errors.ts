import { en } from "../en";

export const errors: (typeof en)["errors"] = {
  wrongCredentials:
    "Введено неправильное имя пользователя или пароль. Попробуйте\u00A0еще\u00A0раз.",
  sessionExpired: "Сессия истекла\nВойдите снова чтобы продолжить работу",
};
