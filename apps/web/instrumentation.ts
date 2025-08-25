// apps/web/instrumentation.ts
import { type Instrumentation } from "next";
import * as Sentry from "@sentry/nextjs";

export async function register() {
  // --- keep your Sentry setup ---
  if (process.env.NODE_ENV === "production") {
    if (process.env.NEXT_PUBLIC_SENTRY_DSN && process.env.NEXT_RUNTIME === "nodejs") {
      await import("./sentry.server.config");
    }
    if (process.env.NEXT_PUBLIC_SENTRY_DSN && process.env.NEXT_RUNTIME === "edge") {
      await import("./sentry.edge.config");
    }
  }

  // --- TLS error instrumentation (Node runtime only) ---
  if (process.env.NEXT_RUNTIME === "nodejs") {
    // 1) Patch Node http/https (use "http"/"https", NOT "node:http")
    try {
      const http = await import("http");
      const https = await import("https");

      const wrapReq = (orig: any, proto: "http" | "https") =>
        function wrapped(options: any, cb?: any) {
          let url: string | undefined;
          try {
            if (typeof options === "string") url = options;
            else if (options?.href) url = options.href;
            else {
              const host = options?.hostname ?? options?.host ?? "localhost";
              const path = options?.path ?? "/";
              const port = options?.port ? `:${options.port}` : "";
              url = `${proto}://${host}${port}${path}`;
            }
          } catch { url = "(unresolved)"; }

          const req = orig.call(this, options, cb);
          req.on("error", (err: any) => {
            const msg = String(err?.message || "");
            const code = err?.code;
            if (code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(msg)) {
              console.error(`[TLS ${proto}.request]`, { url, code, message: msg });
            }
          });
          return req;
        };

      (http as any).request = wrapReq((http as any).request, "http");
      (http as any).get = wrapReq((http as any).get, "http");
      (https as any).request = wrapReq((https as any).request, "https");
      (https as any).get = wrapReq((https as any).get, "https");
    } catch (e) {
      console.warn("TLS http/https patch skipped:", (e as any)?.message);
    }

    // 2) Patch undici (global fetch) best-effort
    try {
      const undici = await import("undici");
      const orig = undici.getGlobalDispatcher();
      class LoggerDispatcher extends (undici as any).Dispatcher {
        inner: any;
        constructor(inner: any) { super(); this.inner = inner; }
        dispatch(opts: any, handler: any) {
          return this.inner.dispatch(opts, {
            ...handler,
            onError: (err: any) => {
              const msg = String(err?.message || "");
              const code = err?.code;
              if (code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(msg)) {
                console.error("[TLS undici.dispatch]", {
                  origin: String(opts?.origin || ""),
                  path: String(opts?.path || ""),
                  method: String(opts?.method || ""),
                  code,
                  message: msg,
                });
              }
              handler.onError(err);
            },
          });
        }
      }
      undici.setGlobalDispatcher(new LoggerDispatcher(orig));
    } catch (e) {
      console.warn("TLS undici patch skipped:", (e as any)?.message);
    }

    // 3) Patch axios (only if present)
    try {
      const axiosMod = await import("axios");
      const axios = axiosMod?.default;
      if (axios) {
        axios.interceptors.response.use(
          r => r,
          (err) => {
            const msg = String(err?.message || "");
            const code = err?.code;
            if (code === "SELF_SIGNED_CERT_IN_CHAIN" || /self-signed/i.test(msg)) {
              const cfg = err?.config || {};
              console.error("[TLS axios]", {
                baseURL: cfg.baseURL, url: cfg.url, method: cfg.method, code, message: msg,
              });
            }
            return Promise.reject(err);
          }
        );
      }
    } catch { /* axios not used; ignore */ }
  }
}

export const onRequestError: Instrumentation.onRequestError = (err, request, context) => {
  if (process.env.NODE_ENV === "production") {
    Sentry.captureRequestError(err, request, context);
  }
};
