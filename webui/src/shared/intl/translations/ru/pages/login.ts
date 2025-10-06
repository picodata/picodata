import { TPages } from "./types";

export const login: TPages["login"] = {
  form: {
    title: "Вход в систему",
    field: {
      username: {
        title: "Логин",
        placeholder: "Введите логин",
      },
      password: {
        title: "Пароль",
        placeholder: "Введите пароль",
      },
      remember: {
        title: "Запомнить меня",
      },

      requiredError: "Обязательно к заполнению",
    },
    loginButton: "Войти",
  },
};
