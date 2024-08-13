export const instances = {
  cluster: {
    plugins: {
      label: "Plugins",
    },
    capacityProgress: {
      label: "Capacity Usage",
      valueLabel: "Useful capacity",
    },
    replicasets: {
      label: "Replicasets",
      description: "total replicasets",
    },
    instances: {
      label: "Instances",
      onlineState: "current state online",
      offlineState: "current state offline",
    },
    version: {
      label: "Version",
      description: "current instance",
    },
  },
  groupBy: {
    options: {
      tiers: "Tiers",
      replicasets: "Replicasets",
      instances: "Instances",
    },
  },
  sortBy: {
    options: {
      name: "Name",
      failureDomain: "Failure Domain",
    },
  },
  filterBy: {
    modal: {
      title: "Filter",
      failureDomainField: {
        label: "Failure Domain",
        promptText:
          "Each parameter must be in KEY-VALUE format. One key can have multiple meanings",
        keyController: {
          placeholder: "Key",
        },
        valueController: {
          placeholder: "Value",
        },
      },
      ok: "Apply",
      clear: "Clear",
    },
  },
  filters: {
    clearAll: "Clear All",
  },
  list: {
    tierCard: {
      name: {
        label: "Tier Name",
      },
      services: {
        label: "Services",
      },
      replicasets: {
        label: "Replicasets",
      },
      instances: {
        label: "Instances",
      },
      rf: {
        label: "RF",
      },
      canVote: {
        label: "Can vote",
      },
    },
    replicasetCard: {
      name: {
        label: "Replicaset Name",
      },
      instances: {
        label: "Instances",
      },
      state: {
        label: "State",
      },
    },
    instanceCard: {
      leader: {
        label: "Leader",
      },
      name: {
        label: "Instance name",
      },
      failureDomain: {
        label: "Failure domain",
      },
      targetState: {
        label: "Target state",
      },
      currentState: {
        label: "Current state",
      },
      binaryAddress: {
        label: "Binary address",
      },
      httpAddress: {
        label: "HTTP address",
      },
      version: {
        label: "Version",
      },
    },
  },
  noData: {
    text: "No Data",
  },
};
