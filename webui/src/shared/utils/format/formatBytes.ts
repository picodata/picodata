export const formatBytes = (bytes: number, decimals = 2) => {
  const k = 1024;
  const sizes = [
    "Bytes",
    "KiB",
    "MiB",
    "GiB",
    "TiB",
    "PiB",
    "EiB",
    "ZiB",
    "YiB",
  ];

  const i = bytes !== 0 ? Math.floor(Math.log(bytes) / Math.log(k)) : 0;

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${
    sizes[i]
  }`;
};
