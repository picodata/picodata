import { en } from "../en";

export const components: (typeof en)["components"] = {
  filterTags: {
    tier: "Тир",
    replicaset: "Репликасет",
    instance: "Инстанс",
    version: "Версия",
    httpAddress: "HTTP Адрес",
    failureDomain: "Домен отказа",
    currentState: "Стейт инстанса",
    leaderState: "Стейт лидера",
    searchForThisText: "Поиск по тексту",
    raftLeader: "Raft leader",
    voter: "Голосует",
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
