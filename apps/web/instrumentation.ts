export async function register() {
  const orig = globalThis.fetch;
  globalThis.fetch = async (input: any, init?: RequestInit) => {
    try {
      return await orig(input, init);
    } catch (e: any) {
      const url = typeof input === "string" ? input : input?.url ?? String(input);
      console.error("[TLS/Fetch error]", {
        url,
        message: e?.message,
        code: e?.code,
        cause: e?.cause?.code || e?.cause
      });
      throw e;
    }
  };
}
