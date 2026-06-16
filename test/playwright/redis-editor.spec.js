"use strict";

const fs = require("fs");
const path = require("path");
const { test, expect } = require("playwright/test");
const {
  deploy,
  openEditor,
  openRedisConfig,
  runPingCheck,
  saveConfigDialog,
  setCodeEditorValue,
  setInputValue,
  setSelectValue,
  startNodeRed,
} = require("./helpers/node-red-editor");

function noauthOptions() {
  return JSON.parse(process.env.REDIS_PLAYWRIGHT_NOAUTH_OPTIONS || "{}");
}

function memoryDbConfigured() {
  return process.env.MEMORYDB_PLAYWRIGHT_ENABLED === "1";
}

function memoryDbOptionsEnvName() {
  return "NODE_RED_REDIS_PLAYWRIGHT_MEMORYDB_OPTIONS";
}

function setMemoryDbOptionsEnv() {
  process.env[memoryDbOptionsEnvName()] = JSON.stringify([
    {
      dnsLookupStrategy: "identity",
      host: process.env.MEMORYDB_ENDPOINT,
      port: Number(process.env.MEMORYDB_PORT || 6379),
      username: process.env.MEMORYDB_USERNAME,
      password: process.env.MEMORYDB_PASSWORD,
    },
  ]);
}

async function clickConnectionTest(page, tabSelector) {
  const row = page.locator(`${tabSelector} .redis-config-test-row`);
  await row.locator(".redis-config-test-button").click();
  await expect(row.locator(".redis-config-test-message")).toContainText("PING -> PONG", {
    timeout: 15000,
  });
}

async function useJsonSingleConnection(page) {
  await page.locator("#red-ui-tab-redis-config-tab-connection").click();
  await expect(page.locator("#redis-config-tab-connection")).toBeVisible();
  await setSelectValue(page, "#redis-config-mode", "single");
  await setInputValue(page, "#redis-config-single-host", "127.0.0.1");
  await setInputValue(page, "#redis-config-single-port", "6379");
  await setInputValue(page, "#redis-config-single-username", "");
  await setInputValue(page, "#redis-config-single-password", "");
  await setInputValue(page, "#redis-config-single-db", "");
  await page.locator("#redis-config-single-tls").setChecked(false);
  await clickConnectionTest(page, "#redis-config-tab-connection");
}

async function useEnvConnection(page, envName) {
  await page.locator("#red-ui-tab-redis-config-tab-options").click();
  await expect(page.locator("#redis-config-tab-options")).toBeVisible();
  await setSelectValue(page, "#redis-config-options-type", "env");
  await setInputValue(page, "#redis-config-options-raw", envName);
  await expect(page.locator("#redis-config-single-host")).toBeDisabled();
  await expect(page.locator("#redis-config-single-port")).toBeDisabled();
  await clickConnectionTest(page, "#redis-config-tab-options");
}

async function waitForLibrarySaveFolder(page) {
  await page.waitForFunction(() => {
    const browser = $("#red-ui-library-dialog-save-browser .red-ui-treeList");
    if (browser.length === 0 || typeof browser.treeList !== "function") {
      return false;
    }
    const selected = browser.treeList("selected");
    return selected && Array.isArray(selected.children);
  });
}

test.describe("Node-RED Redis editor", () => {
  let nodeRed;

  test.afterEach(async () => {
    if (nodeRed) {
      await nodeRed.stop();
      nodeRed = null;
    }
  });

  test("redis-config edits JSON, env auth, and env cluster connections in real Node-RED", async ({
    page,
  }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);

    await test.step("Connection tab JSON config reaches noauth standalone Redis", async () => {
      await openRedisConfig(page);
      await useJsonSingleConnection(page);
      await saveConfigDialog(page);
      await deploy(page);
      await runPingCheck(page);
    });

    await test.step("ConnString env config reaches auth standalone Redis", async () => {
      await openRedisConfig(page);
      await useEnvConnection(page, process.env.REDIS_PLAYWRIGHT_AUTH_ENV);
      await saveConfigDialog(page);
      await deploy(page);
      await runPingCheck(page);
    });

    await test.step("saved env config reopens on ConnString tab", async () => {
      await openRedisConfig(page);
      await expect(page.locator("#redis-config-tab-options")).toBeVisible();
      await expect(page.locator("#redis-config-options-type")).toHaveValue("env");
      await expect(page.locator("#redis-config-options-raw")).toHaveValue(
        process.env.REDIS_PLAYWRIGHT_AUTH_ENV
      );
    });

    await test.step("changing env name reaches auth Redis Cluster", async () => {
      await useEnvConnection(page, process.env.REDIS_PLAYWRIGHT_CLUSTER_ENV);
      await saveConfigDialog(page);
      await deploy(page);
      await runPingCheck(page);
    });
  });

  test("redis-config stores the password as a credential, not in the flow", async ({ page }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);

    await openRedisConfig(page);
    await page.locator("#red-ui-tab-redis-config-tab-connection").click();
    await setSelectValue(page, "#redis-config-mode", "single");
    await setInputValue(page, "#redis-config-single-host", "127.0.0.1");
    await setInputValue(page, "#redis-config-single-port", "6379");
    await setInputValue(page, "#redis-config-single-username", "default");
    await setInputValue(page, "#redis-config-single-password", "super-secret-pw");
    await saveConfigDialog(page);
    await deploy(page);

    // The persisted flow must not contain the password anywhere.
    const flowsText = await (await page.request.get(nodeRed.url + "flows")).text();
    expect(flowsText).not.toContain("super-secret-pw");

    // The text credential for the config node holds the secret.
    const cred = await (
      await page.request.get(nodeRed.url + "credentials/redis-config/redis-config-1")
    ).json();
    expect(JSON.stringify(cred)).toContain("super-secret-pw");

    // Reopen and confirm the password round-trips into the form.
    await openRedisConfig(page);
    await page.locator("#red-ui-tab-redis-config-tab-connection").click();
    await expect(page.locator("#redis-config-single-password")).toHaveValue("super-secret-pw");
    await saveConfigDialog(page);
  });

  test("redis-config can target MemoryDB env options when configured", async ({ page }) => {
    test.skip(
      !memoryDbConfigured(),
      process.env.MEMORYDB_PLAYWRIGHT_SKIP_REASON || "MEMORYDB_* variables are not all set"
    );
    setMemoryDbOptionsEnv();

    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    await openRedisConfig(page);
    await useEnvConnection(page, memoryDbOptionsEnvName());
    await saveConfigDialog(page);
    await deploy(page);
    await runPingCheck(page);
  });

  test("redis-lua-script library save uses real lookup menu and checkbox metadata", async ({
    page,
  }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    await page.evaluate(() => {
      RED.nodes.import(
        [
          {
            id: "lua1",
            type: "redis-lua-script",
            z: "flow1",
            server: "redis-config-1",
            name: "",
            keyval: 0,
            func: "\nreturn nil",
            stored: false,
            block: false,
            wires: [[]],
            x: 250,
            y: 200,
          },
        ],
        { markChanged: true }
      );
      RED.view.redraw(true);
      RED.editor.edit(RED.nodes.node("lua1"));
    });

    await page.waitForSelector("#node-input-func-editor", { state: "visible" });
    await expect(page.locator("#node-input-lua-lookup")).toBeVisible();
    await setInputValue(page, "#node-input-name", "stored block test");
    await setInputValue(page, "#node-input-keyval", "2");
    await page.locator("#node-input-stored").setChecked(true);
    await page.locator("#node-input-block").setChecked(true);
    await setCodeEditorValue(page, "node-input-func-editor", "return redis.call('PING')");

    await page.selectOption("#node-input-mode", "function");
    await expect(page.locator("#redis-lua-fname-row")).toBeVisible();
    await expect(page.locator("#redis-lua-stored-row")).toBeHidden();
    await setInputValue(page, "#node-input-fname", "demofn");
    await page.locator("#node-input-readonly").setChecked(true);

    await page.locator("#node-input-lua-lookup").click();
    await page.locator("#node-input-lua-menu-save-library").click();
    await expect(page.locator("#red-ui-library-dialog-save")).toBeVisible();
    await expect(page.locator("#red-ui-library-dialog-save-filename")).toHaveValue(
      "stored-block-test.lua"
    );
    await expect(page.locator("#red-ui-library-dialog-save-button")).toBeEnabled();
    await waitForLibrarySaveFolder(page);
    const saveResponsePromise = page.waitForResponse((response) => {
      return (
        response.request().method() === "POST" &&
        response.url().includes("/library/local/functions/")
      );
    });
    await page.locator("#red-ui-library-dialog-save-button").click();
    const saveResponse = await saveResponsePromise;
    expect(saveResponse.ok()).toBeTruthy();

    const savedFile = path.join(nodeRed.userDir, "lib", "functions", "stored-block-test.lua");
    await expect
      .poll(() => fs.existsSync(savedFile), { timeout: 5000 })
      .toBe(true);
    const saved = fs.readFileSync(savedFile, "utf8");
    expect(saved).toContain("// name: stored block test");
    expect(saved).toContain("// keyval: 2");
    expect(saved).toContain("// stored: true");
    expect(saved).toContain("// block: true");
    expect(saved).toContain("// mode: function");
    expect(saved).toContain("// readonly: true");
    expect(saved).toContain("// fname: demofn");
    expect(saved).toContain("return redis.call('PING')");
  });

  test("redis-lua-script fresh node gets a library template when switched to Function mode", async ({
    page,
  }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    // A freshly dropped node: all defaults, script-mode starter content.
    await page.evaluate(() => {
      RED.nodes.import(
        [
          {
            id: "lua-fresh",
            type: "redis-lua-script",
            z: "flow1",
            server: "redis-config-1",
            name: "",
            keyval: 0,
            func: "\nreturn nil",
            stored: false,
            block: false,
            mode: "script",
            readonly: false,
            fname: "",
            wires: [[]],
            x: 250,
            y: 200,
          },
        ],
        { markChanged: true }
      );
      RED.view.redraw(true);
      RED.editor.edit(RED.nodes.node("lua-fresh"));
    });

    await page.waitForSelector("#node-input-func-editor", { state: "visible" });
    await page.selectOption("#node-input-mode", "function");

    // The pristine script template is replaced by a working library template
    // and the function name is pre-filled to match it.
    await expect(page.locator("#node-input-fname")).toHaveValue("myfunc");

    await page.getByRole("button", { name: "Done", exact: true }).click();
    const savedFunc = await page.evaluate(() => RED.nodes.node("lua-fresh").func);
    expect(savedFunc).toContain("#!lua name=");
    expect(savedFunc).toContain("redis.register_function");
  });

  test("redis-lua-script editor toggles function-mode fields with the mode select", async ({
    page,
  }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    await page.evaluate(() => {
      RED.nodes.import(
        [
          {
            id: "lua-fn",
            type: "redis-lua-script",
            z: "flow1",
            server: "redis-config-1",
            name: "",
            keyval: 1,
            func: "#!lua name=demolib\nredis.register_function('demofn', function(keys, args) return redis.call('GET', keys[1]) end)",
            stored: false,
            block: false,
            mode: "function",
            readonly: false,
            fname: "demofn",
            wires: [[]],
            x: 250,
            y: 200,
          },
        ],
        { markChanged: true }
      );
      RED.view.redraw(true);
      RED.editor.edit(RED.nodes.node("lua-fn"));
    });

    await page.waitForSelector("#node-input-func-editor", { state: "visible" });

    // A saved Function-mode node opens with the function fields shown and the
    // stored checkbox hidden — oneditprepare applies visibility from saved state.
    await expect(page.locator("#node-input-mode")).toHaveValue("function");
    await expect(page.locator("#node-input-fname")).toHaveValue("demofn");
    await expect(page.locator("#redis-lua-fname-row")).toBeVisible();
    await expect(page.locator("#redis-lua-stored-row")).toBeHidden();
    await expect(page.locator("#redis-lua-editor-label")).toHaveText("Lua Library");

    // Switching to Script mode flips visibility (Stored shown, Function hidden)...
    await page.selectOption("#node-input-mode", "script");
    await expect(page.locator("#redis-lua-stored-row")).toBeVisible();
    await expect(page.locator("#redis-lua-fname-row")).toBeHidden();
    await expect(page.locator("#redis-lua-editor-label")).toHaveText("Lua Script");

    // ...and switching back to Function mode flips it again.
    await page.selectOption("#node-input-mode", "function");
    await expect(page.locator("#redis-lua-fname-row")).toBeVisible();
    await expect(page.locator("#redis-lua-stored-row")).toBeHidden();
  });

  test("redis-in editor toggles command-specific fields", async ({ page }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    await page.evaluate(() => {
      RED.nodes.import(
        [
          {
            id: "redis-in-1",
            type: "redis-in",
            z: "flow1",
            server: "redis-config-1",
            command: "blpop",
            name: "",
            topic: "queue",
            obj: false,
            timeout: 1,
            groupname: "",
            consumername: "",
            wires: [[]],
            x: 250,
            y: 260,
          },
        ],
        { markChanged: true }
      );
      RED.view.redraw(true);
      RED.editor.edit(RED.nodes.node("redis-in-1"));
    });

    await page.waitForSelector("#node-input-command", { state: "visible" });
    await expect(page.locator("#node-input-timeout-row")).toBeVisible();
    await expect(page.locator("#node-input-groupname-row")).toBeHidden();
    await expect(page.locator("#node-input-consumername-row")).toBeHidden();

    await setSelectValue(page, "#node-input-command", "subscribe");
    await expect(page.locator("#node-input-timeout-row")).toBeHidden();
    await expect(page.locator("#node-input-groupname-row")).toBeHidden();
    await expect(page.locator("#node-input-consumername-row")).toBeHidden();

    await setSelectValue(page, "#node-input-command", "xreadgroup");
    await expect(page.locator("#node-input-timeout-row")).toBeHidden();
    await expect(page.locator("#node-input-groupname-row")).toBeVisible();
    await expect(page.locator("#node-input-consumername-row")).toBeVisible();
  });

  test("redis-command editor initializes JSON typedInput params", async ({ page }) => {
    nodeRed = await startNodeRed(noauthOptions());
    await openEditor(page, nodeRed.url);
    await page.evaluate(() => {
      RED.editor.edit(RED.nodes.node("redis-ping"));
    });
    await page.waitForSelector("#node-input-params", { state: "attached" });

    await expect(page.locator("#node-input-paramsType")).toHaveValue("json");
    await expect(page.locator(".red-ui-typedInput-container")).toBeVisible();

    await page.evaluate(() => {
      $("#node-input-params").typedInput("value", "[\"hello\"]");
      $("#node-input-params").trigger("change");
    });
    await page.locator("#node-dialog-ok").click();
    await page.locator("#node-dialog-ok").waitFor({ state: "hidden" });

    const saved = await page.evaluate(() => {
      const node = RED.nodes.node("redis-ping");
      return {
        params: node.params,
        paramsType: node.paramsType,
      };
    });
    expect(saved).toEqual({ params: '["hello"]', paramsType: "json" });
  });
});
