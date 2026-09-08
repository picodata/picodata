export const instances = {
  cluster: {
    plugins: {
      label: "Plugins",
    },
    capacityProgress: {
      label: "Used space in the cluster",
      valueLabel: "Useful capacity",
    },
    systemCapacityProgress: {
      label: "System Capacity Usage",
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
      description: "cluster",
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
    common: {
      hasRaftLeader: "Has governor",
      hasVoter: "Has voter",
      memory: {
        label: "Memory",
        infoTitle: "How memory usage is calculated",
        infoDescriptionReplicaset:
          "The percentage shows how much of the available memory is already allocated on the replicaset's leader instance.",
        infoDescriptionTier:
          "The percentage shows how much of the available memory is already allocated across the tier: for each replicaset, the amount allocated on its leader instance is summed across all replicasets in the tier.",
        infoFreedMemoryNote:
          "Can also include memory that was allocated for data and later freed but not yet returned by the allocator — for example, after a TRUNCATE TABLE, the freed memory may still be reported as used.",
        infoRestartNote:
          "This value may drop after an instance restarts. Freed memory isn't returned to the OS during normal operation — only on restart.",
      },
    },
    tierCard: {
      name: {
        label: "Tier Name",
      },
      services: {
        label: "Services",
        noServices: "No services",
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
      bucket_count: {
        label: "Buckets",
      },
      canVote: {
        label: "Can vote",
      },
      statuses: {
        voter: {
          label: "can vote",
        },
      },
    },
    replicasetCard: {
      name: {
        label: "Replicaset Name",
      },
      instances: {
        label: "Instances",
        outOf: "out of",
      },
      state: {
        label: "State",
      },
      replicasetStateNotReady: {
        label: "Not ready",
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
        label: "State",
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
        label: "Version",
      },
    },
    fullInstanceCard: {
      instance: {
        label: "Instance",
      },
      tabs: {
        common: "Common",
        storage: "Storage",
        replication: "Replication",
      },
      commonContent: {
        basic: "Basic information",
        tier: "Tier",
        name: "Name",
        replicaset: "Replicaset",
        folders: "Folders",
        addresses: "Addresses",
        statuses: "Statuses",
        raftLeader: "Governor",
        leader: "Leader",
        voter: "Voter",
        log: "Log",
        state: "State",
        picodataVersion: "Picodata Version",
        currentState: "State (current)",
        targetState: "State (target)",
      },
      errorMessage: {
        title: "Failed to connect to the instance",
        firstDescription: "The instance is unavailable or not responding.",
        secondDescription: "Check the node status and network connection.",
      },
      replicationContent: {
        remoteInstance: "Remote instance",
        currentInstance: "Current instance",
        downStreamDescription: "Output connection",
        upStreamDescription: "Input connection",
        connectionInfoUnavailable: "Connection information is unavailable",
      },
    },
  },
  noData: {
    text: "No Data",
  },
};
