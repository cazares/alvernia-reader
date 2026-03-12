import test from "node:test";
import assert from "node:assert/strict";

import {
  deriveDevServerHost,
  isLocalNetworkHost,
  isLoopbackHost,
  resolveFailoverApiBaseUrl,
  resolveLocalApiBaseUrl,
  selectConfiguredApiBaseUrl,
} from "../src/localApiHost.js";

test("isLoopbackHost recognizes localhost-style API urls", () => {
  assert.equal(isLoopbackHost("http://127.0.0.1:8000"), true);
  assert.equal(isLoopbackHost("http://localhost:8000"), true);
  assert.equal(isLoopbackHost("http://192.168.1.197:8000"), false);
});

test("isLocalNetworkHost recognizes loopback and private LAN hosts", () => {
  assert.equal(isLocalNetworkHost("http://127.0.0.1:8000"), true);
  assert.equal(isLocalNetworkHost("http://192.168.1.197:8000"), true);
  assert.equal(isLocalNetworkHost("http://10.0.0.22:8000"), true);
  assert.equal(isLocalNetworkHost("https://mixterious.example.com"), false);
});

test("deriveDevServerHost extracts a non-loopback host from Expo-style candidates", () => {
  assert.equal(
    deriveDevServerHost([
      "",
      "http://127.0.0.1:8081",
      "exp://192.168.1.197:8081",
      "http://localhost:8081",
    ]),
    "192.168.1.197"
  );
});

test("resolveLocalApiBaseUrl preserves non-local configured hosts", () => {
  assert.equal(
    resolveLocalApiBaseUrl({
      configuredBaseUrl: "https://api.miguelscode.com",
      devServerCandidates: ["exp://192.168.1.197:8081"],
    }),
    "https://api.miguelscode.com"
  );
});

test("resolveLocalApiBaseUrl swaps loopback host for dev server host when available", () => {
  assert.equal(
    resolveLocalApiBaseUrl({
      configuredBaseUrl: "http://127.0.0.1:8000",
      devServerCandidates: ["exp://192.168.1.197:8081"],
    }),
    "http://192.168.1.197:8000"
  );
});

test("selectConfiguredApiBaseUrl prefers Metro env over baked expo config", () => {
  assert.equal(
    selectConfiguredApiBaseUrl({
      envBaseUrl: "http://192.168.1.197:8000",
      expoConfigBaseUrl: "http://127.0.0.1:8000",
      defaultBaseUrl: "http://localhost:8000",
    }),
    "http://192.168.1.197:8000"
  );
});

test("resolveFailoverApiBaseUrl refuses to fall back from a remote primary to localhost", () => {
  assert.equal(
    resolveFailoverApiBaseUrl({
      currentBaseUrl: "https://mixterious.example.com",
      primaryBaseUrl: "https://mixterious.example.com",
      fallbackBaseUrl: "http://127.0.0.1:8000",
    }),
    ""
  );
});

test("resolveFailoverApiBaseUrl can switch back to the primary remote host", () => {
  assert.equal(
    resolveFailoverApiBaseUrl({
      currentBaseUrl: "https://backup.mixterious.example.com",
      primaryBaseUrl: "https://mixterious.example.com",
      fallbackBaseUrl: "http://127.0.0.1:8000",
    }),
    "https://mixterious.example.com"
  );
});

test("resolveFailoverApiBaseUrl still allows local fallback when both hosts are local-style", () => {
  assert.equal(
    resolveFailoverApiBaseUrl({
      currentBaseUrl: "http://192.168.1.197:8000",
      primaryBaseUrl: "http://192.168.1.197:8000",
      fallbackBaseUrl: "http://127.0.0.1:8000",
    }),
    "http://127.0.0.1:8000"
  );
});
