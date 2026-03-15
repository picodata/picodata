import { en } from "../en";

export const components: (typeof en)["components"] = {
  filterTags: {
    tier: "Тир",
    replicaset: "Репликасет",
    instance: "Инстанс",
    version: "Версия",
    httpAddress: "HTTP Адрес",
    failureDomain: "Домен отказа",
    currentState: "Состояние инстанса",
    leaderState: "Состояние лидера",
    searchForThisText: "Поиск по тексту",
  },
  networkState: {
    label: {
      online: "онлайн",
      offline: "офлайн",
      expelled: "исключен",
    },
  },
  buttons: {
    groupBy: {
      label: "Группировать",
    },
    filterBy: {
      label: "Фильтр",
    },
    sortBy: {
      label: "Сортировать",
    },
  },
  infoNoData: {
    label: "Нет данных",
  },
  signout: "Выйти",
};
