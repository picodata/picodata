import { TPages } from "./types";

export const instances: TPages["instances"] = {
  cluster: {
    plugins: {
      label: "Плагины",
    },
    capacityProgress: {
      label: "Потребление памяти",
      valueLabel: "Использовано",
    },
    systemCapacityProgress: {
      label: "Потребление системной памяти",
      valueLabel: "Использовано",
    },
    replicasets: {
      label: "Репликасеты",
      description: "общее количество",
    },
    instances: {
      label: "Инстансы",
      onlineState: "онлайн",
      offlineState: "офлайн",
    },
    version: {
      label: "Версия",
      description: "текущего инстанса",
    },
  },
  groupBy: {
    options: {
      tiers: "Тиры",
      replicasets: "Репликасеты",
      instances: "Инстансы",
    },
  },
  sortBy: {
    options: {
      name: "Название",
      failureDomain: "Домен отказа",
    },
  },
  filterBy: {
    modal: {
      title: "Фильтр",
      failureDomainField: {
        label: "Домен отказа",
        promptText:
          "Каждый параметр должен быть в формате КЛЮЧ-ЗНАЧЕНИЕ. Один ключ может иметь несколько значений",
        keyController: {
          placeholder: "Ключ",
        },
        valueController: {
          placeholder: "Значение",
        },
      },
      ok: "Применить",
      clear: "Сбросить",
    },
  },
  filters: {
    clearAll: "Сбросить все",
  },
  list: {
    tierCard: {
      name: {
        label: "Название тира",
      },
      services: {
        label: "Сервисы",
      },
      replicasets: {
        label: "Репликасеты",
      },
      instances: {
        label: "Инстансы",
      },
      rf: {
        label: "Фактор репликации",
      },
      bucket_count: {
        label: "Бакеты",
      },
      canVote: {
        label: "Голосует?",
      },
    },
    replicasetCard: {
      name: {
        label: "Название репликасета",
      },
      instances: {
        label: "Инстансы",
      },
      state: {
        label: "Состояние лидера",
      },
    },
    instanceCard: {
      leader: {
        label: "Лидер",
      },
      name: {
        label: "Название инстанса",
      },
      failureDomain: {
        label: "Домен отказа",
      },
      targetState: {
        label: "Целевое состояние",
      },
      currentState: {
        label: "Текущее состояние",
      },
      binaryAddress: {
        label: "IPROTO",
      },
      httpAddress: {
        label: "HTTP",
      },
      pgAddress: {
        label: "PG",
      },
      version: {
        label: "Версия",
      },
    },
  },
  noData: {
    text: "Нет данных",
  },
};
