export const formatFailDomain = (domain: { key: string; value: string }) => {
  return `${domain.key}: ${domain.value}`;
};

export const formatFailDomains = (
  domains: Array<{ key: string; value: string }>
) => {
  return domains.map(formatFailDomain).join(", ");
};
