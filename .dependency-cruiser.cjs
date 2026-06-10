/**
 * dependency-cruiser configuration for tmuxcc-broker.
 *
 * Enforces the polyrepo boundary invariant:
 *
 *   server-proxy-no-daemon-runtime: server-proxy/src/ must not import @tmuxcc/session-proxy
 *   internal sub-paths (e.g. @tmuxcc/session-proxy/src/runtime/…). Only the
 *   package barrel (@tmuxcc/session-proxy) is allowed.
 *
 * Rationale: @tmuxcc/session-proxy barrel is the stable contract seam.
 * Importing internal sub-paths bypasses the exports map and creates
 * invisible coupling to session-proxy internals.
 */
/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: "server-proxy-no-session-proxy-runtime",
      comment:
        "server-proxy/src/ must only import from the @tmuxcc/session-proxy barrel. " +
        "Sub-path imports like @tmuxcc/session-proxy/src/runtime/… bypass the " +
        "exports map and couple the server-proxy to session-proxy internals.",
      severity: "error",
      from: { path: "^src/" },
      to: {
        path: "@tmuxcc/session-proxy/src/",
      },
    },
  ],
  options: {
    moduleSystems: ["es6", "cjs"],
    tsPreCompilationDeps: true,
    enhancedResolveOptions: {
      exportsFields: ["exports"],
      conditionNames: ["import", "require", "node", "default"],
      extensions: [".ts", ".js", ".mts", ".mjs"],
    },
    reporterOptions: {
      text: {
        highlightFocused: true,
      },
    },
  },
};
