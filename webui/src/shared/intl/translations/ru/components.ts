import { en } from "../en";

export const components: (typeof en)["components"] = {
  networkState: {
    label: {
      online: "онлайн",
      offline: "офлайн",
      unknown: "неизвестно",
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
};
