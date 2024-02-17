export const instances = {
  cluster: {
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
      onlineGrade: "current grade online",
      offlineGrade: "current grade offline",
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
      plugins: {
        label: "Plugins",
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
      grade: {
        label: "Grade",
      },
    },
    instanceCard: {
      name: {
        label: "Instance name",
      },
      failureDomain: {
        label: "Failure domain",
      },
      targetGrade: {
        label: "Target grade",
      },
      currentGrade: {
        label: "Current grade",
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
};
