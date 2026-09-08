import { TPages } from "./types";

export const instances: TPages["instances"] = {
  cluster: {
    plugins: {
      label: "Плагины",
    },
    capacityProgress: {
      label: "Использовано места в кластере",
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
      description: "кластера",
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
    common: {
      hasRaftLeader: "Включает в себя Губернатора",
      hasVoter: "Включает в себя голосующий инстанс",
      memory: {
        label: "Память",
        infoTitle: "Как считается использование памяти",
        infoDescriptionReplicaset:
          "Процент показывает, какая часть доступной памяти уже аллоцирована на лидере репликасета.",
        infoDescriptionTier:
          "Процент показывает, какая часть доступной памяти уже аллоцирована в тире: по каждому репликасету учитывается объём, аллоцированный на его лидере, значения суммируются по всем репликасетам тира.",
        infoFreedMemoryNote:
          "Может включать память, которая была выделена под данные, а затем освобождена, но ещё не возвращена аллокатором — например, после TRUNCATE TABLE освобождённая память всё ещё может отображаться как занятая.",
        infoRestartNote:
          "Это значение может снижаться после перезапуска инстанса. В процессе работы освобождённая память не возвращается операционной системе — это происходит только при перезапуске.",
      },
    },
    tierCard: {
      name: {
        label: "Название тира",
      },
      services: {
        label: "Сервисы",
        noServices: "Нет сервисов",
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
      statuses: {
        voter: {
          label: "Голосует",
        },
      },
    },
    replicasetCard: {
      name: {
        label: "Название репликасета",
      },
      instances: {
        label: "Инстансы",
        outOf: "из",
      },
      state: {
        label: "Стейт лидера",
      },
      replicasetStateNotReady: {
        label: "Не готов",
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
        label: "Стейт",
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
    fullInstanceCard: {
      instance: {
        label: "Инстанс",
      },
      tabs: {
        common: "Общее",
        storage: "Хранилище",
        replication: "Репликация",
      },
      commonContent: {
        basic: "Основная информация",
        tier: "Тир",
        name: "Имя",
        replicaset: "Репликасет",
        folders: "Директории",
        addresses: "Адреса",
        statuses: "Статусы",
        raftLeader: "Губернатор",
        leader: "Лидер",
        voter: "Голосующий",
        log: "Лог",
        state: "Стейт",
        picodataVersion: "Версия Picodata",
        currentState: "Стейт (текущий)",
        targetState: "Стейт (целевой)",
      },
      errorMessage: {
        title: "Не удалось подключиться к инстансу",
        firstDescription: "Инстанс не доступен или не отвечает.",
        secondDescription: "Проверьте состояние узла и сетевое соединение.",
      },
      replicationContent: {
        remoteInstance: "Удалённый инстанс",
        currentInstance: "Текущий инстанс",
        downStreamDescription: "Исходящее соединение",
        upStreamDescription: "Входящее соединение",
        connectionInfoUnavailable: "Информация о соединении недоступна",
      },
    },
  },
  noData: {
    text: "Нет данных",
  },
};
