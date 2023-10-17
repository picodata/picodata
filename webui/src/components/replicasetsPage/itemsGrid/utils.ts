export const formatFailDomain = (
  domains: Array<{ key: string; value: string }>
) => {
  return domains.map((domain) => `${domain.key}: ${domain.value}`).join(", ");
};
