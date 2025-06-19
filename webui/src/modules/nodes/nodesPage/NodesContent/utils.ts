export const formatFailDomain = (domain: {
  key: string;
  value: string | string[];
}) => {
  return `${domain.key}: ${
    Array.isArray(domain.value)
      ? domain.value.slice().sort().join(", ")
      : domain.value
  }`;
};

export const formatFailDomains = (
  domains: Array<{ key: string; value: string }>
) => {
  return domains.map(formatFailDomain).sort().join(", ");
};
