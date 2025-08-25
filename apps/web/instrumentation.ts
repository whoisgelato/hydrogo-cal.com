// apps/web/instrumentation.ts
export async function register() {
  // ---- Patch Node http/https ----
  const http = await import("node:http");
  const https = await import("node:https");

  const wrapReq = (orig: any, proto: "http" | "https") =>
    function wrapped(options: any, cb?: any) {
      try {
        // Normalize URL for logging
        let url: string | undefined;
        if (typeof options === "string") url = options;
        else if (options?.href) url = options.href;
        else {
          const host = options?.hostname ?? options?.host ?? "localhost";
          const path = options?.path ?? "/";
          const port = options?.port ? `:${options.port}` : "";
          url = `${proto}://${host}${port}${path}`;
        }
        const req = orig.call(this, options, cb);
        req.on("error", (err: any) => {
          if (err?.code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(err?.message || "")) {
            console.error(`[TLS ${proto}.request]`, { url, code: err.code, message: err.message });
          }
        });
        return req;
      } catch (e: any) {
        console.error(`[TLS ${proto}.request] wrap error`, e?.message);
        return orig.call(this, options, cb);
      }
    };

  // Patch request & get for both http and https
  (http as any).request = wrapReq((http as any).request, "http");
  (http as any).get = wrapReq((http as any).get, "http");
  (https as any).request = wrapReq((https as any).request, "https");
  (https as any).get = wrapReq((https as any).get, "https");

  // ---- Patch undici global dispatcher (fetch uses this) ----
  try {
    const undici = await import("undici");
    const origDisp = undici.getGlobalDispatcher();
    class LoggerDispatcher extends (undici as any).Dispatcher {
      inner: any;
      constructor(inner: any) { super(); this.inner = inner; }
      dispatch(opts: any, handler: any) {
        return this.inner.dispatch(opts, {
          ...handler,
          onError: (err: any) => {
            if (err?.code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(err?.message || "")) {
              console.error("[TLS undici.dispatch]", {
                origin: String(opts?.origin || ""),
                path: String(opts?.path || ""),
                method: String(opts?.method || ""),
                code: err.code,
                message: err.message
              });
            }
            handler.onError(err);
          }
        });
      }
    }
    undici.setGlobalDispatcher(new LoggerDispatcher(origDisp));
  } catch { /* undici patch best-effort */ }

  // ---- Patch axios if present ----
  try {
    const axios = (await import("axios")).default;
    axios.interceptors.response.use(
      (r) => r,
      (err) => {
        const cfg = err?.config || {};
        if (err?.code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(err?.message || "")) {
          console.error("[TLS axios]", {
            baseURL: cfg.baseURL,
            url: cfg.url,
            method: cfg.method,
            code: err.code,
            message: err.message,
          });
        }
        return Promise.reject(err);
      }
    );
  } catch { /* axios may not be used; ignore */ }
}
